# -*- coding: utf-8 -*-
import base64
import logging
import os
import tempfile
import xmlrpc.client
import json
import time

import psycopg2
from PIL import Image

from odoo import models, api, fields
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class ProductMigration(models.Model):
    _name = 'product.migration'
    _description = 'Migración de Productos'

    url = fields.Char(string="url")
    db = fields.Char(string="Base de Datos")
    username = fields.Char(string="Usuario")
    password = fields.Char(string="Contraseña")

    # =========================
    # CONEXIÓN Y UTILIDADES
    # =========================
    def connect_to_odoo(self):
        try:
            common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(self.url))
            uid = common.authenticate(self.db, self.username, self.password, {})
            models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self.url))
            return uid, models
        except Exception as e:
            _logger.error('Error al conectar a Odoo : %s', e)
            raise

    def execute_kw_with_retry(self, models, db, uid, password, model, method, args, kwargs=None):
        if kwargs is None:
            kwargs = {}
        max_retries = 5
        wait_seconds = 5
        for attempt in range(max_retries):
            try:
                return models.execute_kw(db, uid, password, model, method, args, kwargs)
            except xmlrpc.client.ProtocolError as e:
                if getattr(e, 'errcode', None) == 429:  # Too Many Requests
                    _logger.warning(f"429 Too Many Requests, esperando {wait_seconds}s antes de reintentar...")
                    time.sleep(wait_seconds)
                    wait_seconds = min(wait_seconds * 2, 60)
                else:
                    raise
        raise Exception("Max retries reached for execute_kw_with_retry")

    def _get_local_publish_field(self):
        """Devuelve 'is_published' si existe en local; de lo contrario None (no publicar)."""
        return 'is_published' if self._local_field_exists('product.template', 'is_published') else None


    def _find_local_category_id(self, remote_categ):
        """
        remote_categ llega como (id, name) en search_read de XML-RPC.
        Busca por nombre en product.category local.
        Ajusta a complete_name si tu mapeo lo requiere.
        """
        if isinstance(remote_categ, (list, tuple)) and len(remote_categ) == 2:
            name = remote_categ[1]
            if name:
                cat = self.env['product.category'].sudo().search([('name', '=', name)], limit=1)
                return cat.id or False
        return False

    def _build_product_vals(self, product_data):
        """
        Vals seguros para product.template:
        - NO usa website_published.
        - NO setea barcode en template (lo pondremos luego en la variante).
        - Setea company_id explícito si target_company_id está definido.
        """
        vals = {
            'name': product_data.get('name'),
            'default_code': product_data.get('default_code') or False,
            'list_price': product_data.get('list_price') or 0.0,
            'standard_price': product_data.get('standard_price') or 0.0,
            'active': product_data.get('active', True),
            # 'barcode' -> se setea en la variante luego
        }
        # compañía explícita
        if self.target_company_id:
            vals['company_id'] = self.target_company_id.id

        # categoría (por nombre simple)
        categ = product_data.get('categ_id')
        if categ and isinstance(categ, (list, tuple)) and len(categ) >= 2:
            name = categ[1]
            cat = self.env['product.category'].sudo().search([('name', '=', name)], limit=1)
            if not cat:
                cat = self.env['product.category'].sudo().create({'name': name})
            vals['categ_id'] = cat.id

        PT = 'product.template'
        if self._local_field_exists(PT, 'sale_ok'):
            vals['sale_ok'] = bool(product_data.get('sale_ok', True))
        if self._local_field_exists(PT, 'available_in_pos'):
            vals['available_in_pos'] = bool(product_data.get('available_in_pos', False))
        if self._local_field_exists(PT, 'is_published'):
            vals['is_published'] = bool(product_data.get('is_published', False))

        return vals


    # =========================
    # MIGRACIÓN DE IMÁGENES (128)
    # =========================
    def copiar_imagenes_productos(self):
        """
        Copia las imágenes de productos de tamaño 128x128 desde remoto a local.
        Busca por nombre y setea image_128 si no existe en local.
        """
        uid, models = self.connect_to_odoo()

        try:
            productos_remoto = self.execute_kw_with_retry(
                models, self.db, uid, self.password,
                'product.template', 'search', [[('name', '!=', False)]],
            )
            _logger.info(f"Total de productos para migrar imagen 128: {len(productos_remoto)}")

            for product_id in productos_remoto:
                if product_id > 0:
                    datos_imagen = self.execute_kw_with_retry(
                        models, self.db, uid, self.password,
                        'product.template', 'read', [[product_id], ['image_128', 'name']]
                    )
                    if datos_imagen and datos_imagen[0].get('image_128'):
                        producto_local = self.env['product.template'].search([('name', '=', datos_imagen[0]['name'])], limit=1)
                        if producto_local and not producto_local.image_128:
                            producto_local.image_128 = datos_imagen[0]['image_128']
                            _logger.info(f"Imagen 128 actualizada para '{producto_local.name}'.")

            _logger.info("Migración de imágenes 128 completada.")

        except Exception as e:
            _logger.error(f"Error durante la migración de imágenes 128: {e}")

    # =========================
    # MIGRACIÓN DE CONTACTOS
    # =========================
    # =========================
    # CONTACTOS – Helpers
    # =========================
# ===== Helpers de matching por nombre (pegar dentro de ProductMigration) =====

    def _norm_name(self, s):
        s = (s or '').strip().lower()
        # colapsa espacios múltiples
        return ' '.join(s.split())

    def _norm_str(self, v):
        return (v or '').strip()

    def _norm_email(self, v):
        return self._norm_str(v).lower()

    def _norm_rut(self, v):
        """Normaliza RUT: sin puntos/espacios; DV con guion si falta."""
        if not v:
            return ''
        s = str(v).strip().upper().replace('.', '').replace(' ', '')
        if '-' not in s and len(s) > 1:
            s = f"{s[:-1]}-{s[-1]}"
        return s

    def _find_country_by_name(self, name):
        if not name:
            return False
        C = self.env['res.country'].sudo()
        return C.search([('name', 'ilike', name)], limit=1).id

    def _find_state_by_name(self, country_id, name):
        if not (country_id and name):
            return False
        S = self.env['res.country.state'].sudo()
        return S.search([('country_id', '=', country_id), ('name', 'ilike', name)], limit=1).id

    def _find_city_commune(self, name):
        """Ajusta si usas un modelo propio de comunas."""
        return self._norm_str(name)

    def _find_latam_id_type(self, code_or_name):
        """Busca l10n_latam.identification.type por code o name (si existe)."""
        if not self._local_field_exists('res.partner', 'l10n_latam_identification_type_id'):
            return False
        if not code_or_name:
            return False
        T = self.env['l10n_latam.identification.type'].sudo()
        rec = T.search(['|', ('code', '=', code_or_name), ('name', 'ilike', code_or_name)], limit=1)
        return rec.id or False

    def _find_cl_taxpayer_type(self, sel_value_or_label):
        """Mapea l10n_cl_sii_taxpayer_type aceptando valor o etiqueta (si existe)."""
        field_name = 'l10n_cl_sii_taxpayer_type'
        if not self._local_field_exists('res.partner', field_name):
            return False
        imf = self.env['ir.model.fields'].sudo().search([
            ('model', '=', 'res.partner'), ('name', '=', field_name)
        ], limit=1)
        if not imf or not imf.selection:
            return False
        # selection (value,label) por líneas
        options = {}
        for line in imf.selection.split('\n'):
            if ',' in line:
                k, v = line.split(',', 1)
                options[k] = v
        # match directo por value
        if sel_value_or_label in options:
            return sel_value_or_label
        # match por etiqueta
        for k, label in options.items():
            if sel_value_or_label and sel_value_or_label.lower() in (label or '').lower():
                return k
        return False

    def _build_remote_contact_fields(self, models, db, uid, password):
        """
        Define alias por dato y arma la lista final de lectura según exista en la base remota.
        """
        aliases = {
            'is_company':      ['company_type', 'is_company'],
            'name':            ['name'],
            'street':          ['street'],
            'street2':         ['street2'],
            'city':            ['l10n_cl_city', 'city', 'x_city'],
            'state_id':        ['state_id'],
            'country_id':      ['country_id'],
            'zip':             ['zip', 'x_zip'],
            'phone':           ['phone', 'x_phone'],
            'mobile':          ['mobile', 'x_mobile'],
            'email':           ['email', 'x_email'],
            'website':         ['website', 'x_website'],
            'vat':             ['vat', 'l10n_cl_vat', 'document_number', 'x_rut', 'x_vat'],
            'dte_email':       ['l10n_cl_dte_email', 'dte_email', 'x_dte_email'],
            'giro':            ['l10n_cl_activity_description', 'x_giro', 'activity_description', 'comment'],
            'id_type':         ['l10n_latam_identification_type_id', 'x_idtype'],
            'taxpayer_type':   ['l10n_cl_sii_taxpayer_type', 'x_sii_taxpayer_type', 'taxpayer_type'],
            'function':        ['function', 'x_function'],
        }
        candidates = set()
        for arr in aliases.values():
            candidates.update(arr)
        found = self._remote_fields(models, db, uid, password, 'res.partner', list(candidates))
        mapping = {}
        for logical, arr in aliases.items():
            for f in arr:
                if f in found:
                    mapping[logical] = f
                    break
        to_read = sorted(set(mapping.values()) | {'name'})  # garantizar name
        return mapping, to_read

    def _is_empty_val(self, v):
        """Qué se considera 'vacío' para no pisar datos locales."""
        return v in (False, None, '')

    def _merge_fill_missing(self, record, incoming_vals, include=False):
        """
        Devuelve solo campo:valor a escribir si el valor local está vacío.
        Ignora claves inexistentes en el modelo (a menos que include=True).
        """
        fields_model = record._fields
        out = {}
        for k, v in incoming_vals.items():
            if not include and k not in fields_model:
                continue
            try:
                local_val = record[k]
            except Exception:
                continue
            if self._is_empty_val(local_val) and not self._is_empty_val(v):
                out[k] = v
        return out

    # =========================
    # CONTACTOS – Migración (fill-missing)
    # =========================
    def migrate_contacts(self, commit_every=100, force_overwrite=False):
        """
        Migra contactos desde Odoo remoto.
        Por defecto **NO sobrescribe**: solo completa campos vacíos del contacto local.
        Si `force_overwrite=True`, sobrescribe con los valores remotos.
        """
        uid, models = self.connect_to_odoo()

        # Campos remotos (alias-safe)
        mapping, fields_to_read = self._build_remote_contact_fields(models, self.db, uid, self.password)

        # IDs remotos
        partner_ids = models.execute_kw(self.db, uid, self.password, 'res.partner', 'search', [[]])
        total = len(partner_ids)
        _logger.info(f"[PARTNER] Remotos a procesar: {total}")
        BATCH = 500

        Partner = self.env['res.partner'].sudo()

        # Índices locales por vat y email para acelerar
        vat_idx = {self._norm_rut(p.vat): p.id for p in Partner.search([('vat', '!=', False)])}
        email_idx = {self._norm_email(p.email): p.id for p in Partner.search([('email', '!=', False)])}

        def _find_local(vals_remote):
            vat = self._norm_rut(vals_remote.get('vat'))
            if vat and vat in vat_idx:
                return Partner.browse(vat_idx[vat])
            email = self._norm_email(vals_remote.get('email'))
            if email and email in email_idx:
                return Partner.browse(email_idx[email])
            name = self._norm_str(vals_remote.get('name'))
            city = self._norm_str(vals_remote.get('city'))
            dom = [('name', '=', name)]
            if city:
                dom.append(('city', '=', city))
            return Partner.search(dom, limit=1)

        processed = 0

        for i in range(0, total, BATCH):
            batch_ids = partner_ids[i:i + BATCH]
            partners = models.execute_kw(self.db, uid, self.password, 'res.partner', 'read', [batch_ids, fields_to_read])

            for rp in partners:
                try:
                    # extractor remoto g() usando mapping
                    def g(key, default=None):
                        f = mapping.get(key)
                        return rp.get(f) if f else default

                    name = rp.get('name')
                    vat = self._norm_rut(g('vat'))
                    email = self._norm_email(g('email'))
                    phone = self._norm_str(g('phone'))
                    mobile = self._norm_str(g('mobile'))
                    website = self._norm_str(g('website'))
                    street = self._norm_str(g('street'))
                    street2 = self._norm_str(g('street2'))
                    city_in = self._norm_str(g('city'))
                    zip_in = self._norm_str(g('zip'))
                    dte_email = self._norm_email(g('dte_email'))
                    function = self._norm_str(g('function'))
                    giro = self._norm_str(g('giro'))

                    is_company_val = g('is_company')
                    if isinstance(is_company_val, str):
                        is_company = (is_company_val == 'company')
                    else:
                        is_company = bool(is_company_val)

                    # país/estado remotos (si vienen M2O tomamos el nombre)
                    rc = g('country_id')
                    remote_country_name = rc[1] if isinstance(rc, (list, tuple)) and len(rc) >= 2 else None
                    rs = g('state_id')
                    remote_state_name = rs[1] if isinstance(rs, (list, tuple)) and len(rs) >= 2 else None

                    country_id = self._find_country_by_name(remote_country_name) if remote_country_name else False
                    state_id = self._find_state_by_name(country_id, remote_state_name) if (country_id and remote_state_name) else False
                    city = self._find_city_commune(city_in)

                    # tipos fiscales (opcionales)
                    id_type_id = False
                    id_type_val = g('id_type')
                    if self._local_field_exists('res.partner', 'l10n_latam_identification_type_id'):
                        if isinstance(id_type_val, (list, tuple)) and len(id_type_val) >= 2:
                            id_type_id = self._find_latam_id_type(id_type_val[1])
                        else:
                            id_type_id = self._find_latam_id_type(id_type_val)

                    sii_taxpayer_type = False
                    taxpayer_val = g('taxpayer_type')
                    if self._local_field_exists('res.partner', 'l10n_cl_sii_taxpayer_type'):
                        if isinstance(taxpayer_val, (list, tuple)) and len(taxpayer_val) >= 2:
                            sii_taxpayer_type = self._find_cl_taxpayer_type(taxpayer_val[0]) or self._find_cl_taxpayer_type(taxpayer_val[1])
                        else:
                            sii_taxpayer_type = self._find_cl_taxpayer_type(taxpayer_val)

                    # localizar partner local
                    local = _find_local({'vat': vat, 'email': email, 'name': name, 'city': city})

                    # vals entrantes (completos)
                    incoming = {
                        'is_company': is_company,
                        'name': name,
                        'street': street,
                        'street2': street2,
                        'city': city,
                        'zip': zip_in,
                        'phone': phone,
                        'mobile': mobile,
                        'email': email or False,
                        'website': website or False,
                        'vat': vat or False,
                        'function': function or False,
                    }
                    if country_id:
                        incoming['country_id'] = country_id
                    if state_id:
                        incoming['state_id'] = state_id
                    if dte_email and self._local_field_exists('res.partner', 'l10n_cl_dte_email'):
                        incoming['l10n_cl_dte_email'] = dte_email
                    if giro and self._local_field_exists('res.partner', 'l10n_cl_activity_description'):
                        incoming['l10n_cl_activity_description'] = giro
                    if id_type_id:
                        incoming['l10n_latam_identification_type_id'] = id_type_id
                    if sii_taxpayer_type:
                        incoming['l10n_cl_sii_taxpayer_type'] = sii_taxpayer_type

                    # crear o actualizar respetando datos existentes
                    if local and local.id:
                        vals_write = incoming if force_overwrite \
                                     else self._merge_fill_missing(local, incoming)
                        if vals_write:
                            local.write(vals_write)
                            _logger.info(f"[PARTNER] Actualizado (fill_missing): {local.display_name} -> {list(vals_write.keys())}")
                    else:
                        local = Partner.create(incoming)
                        _logger.info(f"[PARTNER] Creado: {local.display_name}")

                    processed += 1
                    if processed % commit_every == 0:
                        self.env.cr.commit()
                        _logger.info(f"[PARTNER] Proceso parcial: {processed}/{total}")

                except Exception as e:
                    self.env.cr.rollback()
                    _logger.error(f"[PARTNER] Error migrando '{rp.get('name')}' : {e}")

        _logger.info(f"[PARTNER] Finalizado. Total procesados: {processed}")
        return True

    # =========================
    # MIGRACIÓN DE PRODUCTOS (Odoo 18 -> local)
    # =========================
    def migrate_products(self):
        """
        Migra productos desde remoto (12/18) a local (16/15):
        - Sólo lee campos remotos existentes (sin website_published).
        - Crea en la compañía elegida (target_company_id) usando with_company/force_company.
        - Evita write inverso de barcode en template (lo setea en la variante).
        - Commit por lotes para evitar rollback por timeout.
        """
        import time
        BATCH_SIZE = 100
        MAX_SECONDS = 90  # margen para no superar 120s de request
        start = time.time()

        uid, models = self.connect_to_odoo()
        barcodes_seen = set()

        # campos remotos existentes
        candidate_fields = [
            'name', 'default_code', 'list_price', 'standard_price', 'active', 'barcode',
            'categ_id', 'sale_ok', 'is_published', 'available_in_pos'
        ]
        fields_to_read = self._remote_fields(models, self.db, uid, self.password, 'product.template', candidate_fields)
        for must in ('name', 'active'):
            if must not in fields_to_read:
                fields_to_read.append(must)

        total_products = models.execute_kw(self.db, uid, self.password, 'product.template', 'search_count', [[('active', '=', True)]])
        _logger.info('Total de productos activos a migrar: %s', total_products)

        # Contexto silencioso y con compañía forzada (si aplica)
        ctx = dict(self.env.context or {})
        ctx.update({
            'tracking_disable': True,
            'mail_create_nosubscribe': True,
            'mail_notrack': True,
            'mail_auto_subscribe_no_notify': True,
            'no_send_mail': True,
        })
        if self.target_company_id:
            ctx['force_company'] = self.target_company_id.id

        # Helper para crear en compañía correcta
        def _pt_env():
            return self.env['product.template'].with_context(ctx).with_company(self.target_company_id) if self.target_company_id else self.env['product.template'].with_context(ctx)

        created_count = 0
        commit_every = self.force_commit_every or 100

        for offset in range(0, total_products, BATCH_SIZE):
            # corte preventivo por timeout
            if time.time() - start > MAX_SECONDS:
                _logger.info("[PROD] Corte preventivo en offset %s para evitar timeout; reejecuta para continuar.", offset)
                break

            product_batch = models.execute_kw(
                self.db, uid, self.password, 'product.template', 'search_read',
                [[('active', '=', True)]],
                {'fields': fields_to_read, 'limit': BATCH_SIZE, 'offset': offset}
            )

            for pdata in product_batch:
                try:
                    name = pdata.get('name')
                    default_code = pdata.get('default_code')
                    barcode = pdata.get('barcode') or False

                    # evitar duplicados por default_code o name (en la compañía objetivo)
                    if default_code:
                        dom = [('default_code', '=', default_code)]
                    else:
                        dom = [('name', '=', name)]
                    if self.target_company_id:
                        dom = ['|', ('company_id', '=', False), ('company_id', '=', self.target_company_id.id)] + dom

                    existing = self.env['product.template'].sudo().search(dom, limit=1)
                    if existing:
                        _logger.info("Producto ya existe localmente: %s, se omite.", existing.name)
                        continue

                    # evitar duplicado por barcode
                    if barcode:
                        if barcode in barcodes_seen:
                            _logger.warning("Barcode duplicado visto en sesión: %s, se omite.", barcode)
                            continue
                        bdom = [('barcode', '=', barcode)]
                        if self.target_company_id:
                            bdom = ['|', ('company_id', '=', False), ('company_id', '=', self.target_company_id.id)] + bdom
                        if self.env['product.template'].sudo().search(bdom, limit=1):
                            _logger.warning("Barcode ya existe en local: %s, se omite.", barcode)
                            continue
                        barcodes_seen.add(barcode)

                    vals = self._build_product_vals(pdata)
                    # 1) crea template (sin barcode)
                    tpl = _pt_env().sudo().create(vals)

                    # 2) setea barcode directo en la variante (evita inverse lento)
                    if barcode and tpl.product_variant_id:
                        tpl.product_variant_id.with_context(ctx).sudo().write({'barcode': barcode})

                    created_count += 1
                    _logger.info("Producto creado: %s (ID %s)", tpl.name, tpl.id)

                    # commits periódicos para no perder trabajo en caso de timeout
                    if created_count % commit_every == 0:
                        self.env.cr.commit()
                        _logger.info("[PROD] Commit de seguridad tras %s creados.", created_count)

                except Exception as e:
                    self.env.cr.rollback()
                    _logger.error("Error al procesar producto %s: %s", pdata.get('name'), e)

        # commit final
        self.env.cr.commit()
        _logger.info("[PROD] Finalizado. Creados en esta corrida: %s", created_count)
        return True
    # =========================
    # EXPORTACIÓN A JSON (única versión)
    # =========================
    def download_product_batches(self):
        """
        Descarga productos activos desde la base remota (Odoo 18) y guarda cada lote como JSON
        para procesamiento offline en 'product.data.json'.
        """
        uid, models = self.connect_to_odoo()
        BATCH_SIZE = 500

        try:
            total_products = models.execute_kw(
                self.db, uid, self.password,
                'product.template', 'search_count', [[('active', '=', True)]]
            )
            _logger.info(f'Total de productos a exportar: {total_products}')

            fields_to_read = [
                'name', 'default_code', 'list_price', 'standard_price', 'active', 'barcode',
                'categ_id', 'sale_ok', 'is_published', 'website_published', 'available_in_pos'
            ]

            for offset in range(0, total_products, BATCH_SIZE):
                product_batch = models.execute_kw(
                    self.db, uid, self.password,
                    'product.template', 'search_read',
                    [[('active', '=', True)]],
                    {'fields': fields_to_read, 'limit': BATCH_SIZE, 'offset': offset}
                )
                if product_batch:
                    self.env['product.data.json'].sudo().create({'data': json.dumps(product_batch)})
                    _logger.info(f'Lote de productos guardado (offset={offset})')

        except Exception as e:
            _logger.error(f'Error al descargar lotes de productos: {e}')

    # =========================
    # MIGRACIÓN DE IMÁGENES (1920) CON CONVERSIÓN
    # =========================
    def migrate_product_images(self, force=False, prefer_variant=False, only_missing=True, batch_size=100, limit=None):
        """
        Copia imágenes desde Odoo remoto y escribe image_1920 en productos locales.

        - Match local 'smart': x_external_id -> default_code -> barcode -> **nombre robusto**.
        - Lee binarios completos (bin_size=False).
        - Prioriza 1920; si no hay, baja a 1024/512/256/128; si prefer_variant=True, invierte prioridad.
        - Si only_missing=True, sólo actualiza productos locales SIN image_1920.
        - Logs detallados del motivo de skip.
        """
        uid, models = self.connect_to_odoo()

        # 1) IDs remotos
        try:
            remote_ids = self.execute_kw_with_retry(
                models, self.db, uid, self.password,
                'product.template', 'search', [[('active', '=', True)]]
            )
            if limit:
                remote_ids = remote_ids[:limit]
        except Exception as e:
            _logger.error(f"[IMG] No se pudieron obtener IDs remotos: {e}")
            return False

        total = len(remote_ids)
        _logger.info(f"[IMG] Productos remotos a revisar: {total} (batch={batch_size}, limit={limit}, only_missing={only_missing}, force={force}, prefer_variant={prefer_variant})")

        # 2) Campos remotos (incluye binarios)
        base_candidates = [
            'id', 'name', 'default_code', 'barcode',
            'image_1920', 'image_1024', 'image_512', 'image_256', 'image_128',
            'image_variant_1920', 'image_variant_1024', 'image_variant_512', 'image_variant_256', 'image_variant_128',
            'x_external_id',  # si existiera
        ]
        fields_to_read = self._remote_fields(models, self.db, uid, self.password, 'product.template', base_candidates)
        if 'id' not in fields_to_read:
            fields_to_read.insert(0, 'id')
        if 'name' not in fields_to_read:
            fields_to_read.insert(1, 'name')

        import hashlib, base64
        def _sha1_b64(b64str):
            if not b64str:
                return None
            try:
                raw = base64.b64decode(b64str)
            except Exception:
                raw = b64str.encode('utf-8') if isinstance(b64str, str) else b64str
            return hashlib.sha1(raw).hexdigest()

        def _pick_best(self, p, prefer_variant=False):
            """
            Devuelve el mejor base64 disponible, soportando campos modernos (13+)
            y legados (Odoo 12):
            - Modernos plantilla: image_1920/1024/512/256/128
            - Modernos variante:  image_variant_*
            - Legado plantilla:   image, image_medium, image_small
            - Legado variante:    (no existe en 12)
            """
            # Modernos
            def pick(prefix):
                for size in ('1920', '1024', '512', '256', '128'):
                    k = f'{prefix}{size}'
                    if p.get(k):
                        return p[k]
                return None

            # Legado (12)
            legacy = p.get('image') or p.get('image_medium') or p.get('image_small')

            if prefer_variant:
                return pick('image_variant_') or pick('image_') or legacy
            else:
                return pick('image_') or pick('image_variant_') or legacy

        updated = skipped_same = skipped_hasimg = noimg_remote = nomatch_local = errors = 0

        for i in range(0, total, batch_size):
            batch_ids = remote_ids[i:i + batch_size]
            try:
                products = self.execute_kw_with_retry(
                    models, self.db, uid, self.password,
                    'product.template', 'read',
                    [batch_ids, fields_to_read],
                    {'context': {'bin_size': False}}  # <- BIEN: en kwargs.context
                )

            except Exception as e:
                _logger.error(f"[IMG] Fallo leyendo batch {i}-{i+batch_size}: {e}")
                continue

            for p in products:
                try:
                    # 3) Imagen remota
                    img_b64 = _pick_best(p)
                    if not img_b64:
                        noimg_remote += 1
                        continue

                    # 4) Resolver producto local (smart)
                    #    Primero tu método existente (x_external_id/default_code/barcode/name)
                    local_prod = self._find_local_product(p)
                    if not local_prod or not local_prod.id:
                        #    Si no encuentra, forzamos matching robusto por nombre
                        local_prod = self._find_local_product_by_name(p.get('name'))

                    if not local_prod or not local_prod.id:
                        nomatch_local += 1
                        _logger.debug(f"[IMG] Sin match local por nombre: '{p.get('name')}' (id remoto {p.get('id')}).")
                        continue

                    # 5) Filtros de actualización
                    if only_missing and local_prod.image_1920 and not force:
                        skipped_hasimg += 1
                        continue

                    if local_prod.image_1920 and not force:
                        if _sha1_b64(local_prod.image_1920) == _sha1_b64(img_b64):
                            skipped_same += 1
                            continue

                    # 6) Escribir imagen exacta y limpiar thumbs (fuerza recálculo)
                    local_prod.sudo().write({
                        'image_128': False,
                        'image_256': False,
                        'image_512': False,
                        'image_1024': False,
                        'image_1920': img_b64,
                    })
                    updated += 1

                    # commits periódicos
                    if updated % 50 == 0:
                        self.env.cr.commit()
                        _logger.info(f"[IMG] Parcial: updated={updated}, same={skipped_same}, hasimg={skipped_hasimg}, noimg={noimg_remote}, nomatch={nomatch_local}, errors={errors}")

                except Exception as e:
                    errors += 1
                    self.env.cr.rollback()
                    _logger.error(f"[IMG] Error migrando '{p.get('name')}' (remoto {p.get('id')}): {e}")

        self.env.cr.commit()
        _logger.info(f"[IMG] FIN — updated={updated}, same={skipped_same}, hasimg={skipped_hasimg}, noimg_remote={noimg_remote}, nomatch_local={nomatch_local}, errors={errors}")
        return True

    def convertir_y_optimizar_imagen(self, datos_imagen):
        """
        Convierte y optimiza la imagen de WebP a JPEG (quality=85).
        Si falla la conversión, retorna el original en base64.
        """
        try:
            if isinstance(datos_imagen, str):
                raw = base64.b64decode(datos_imagen)
            else:
                raw = datos_imagen

            with tempfile.NamedTemporaryFile(delete=False, suffix=".webp") as temp_file:
                temp_file.write(raw)
                temp_file.flush()
                temp_path = temp_file.name

            try:
                with Image.open(temp_path) as img:
                    # Si ya es JPG/PNG, podríamos evitar recomprimir.
                    fmt = (img.format or '').upper()
                    if fmt in ('JPEG', 'JPG', 'PNG'):
                        with open(temp_path, "rb") as f:
                            return base64.b64encode(f.read()).decode('utf-8')

                    # Convertir a JPEG
                    rgb = img.convert('RGB')
                    out_path = temp_path.replace('.webp', '.jpg')
                    rgb.save(out_path, format='JPEG', optimize=True, quality=85)

                with open(out_path, "rb") as f:
                    optimized_image = base64.b64encode(f.read()).decode('utf-8')
                return optimized_image
            finally:
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    out_path = temp_path.replace('.webp', '.jpg')
                    if os.path.exists(out_path):
                        os.remove(out_path)
                except Exception:
                    pass

        except Exception as e:
            _logger.error(f"Error durante conversión/optimización de imagen: {e}")
            # devolver original seguro
            if isinstance(datos_imagen, bytes):
                return base64.b64encode(datos_imagen).decode('utf-8')
            return datos_imagen

    def _b64_sha1(self, b64str):
        import hashlib, base64
        if not b64str:
            return None
        if isinstance(b64str, str):
            raw = base64.b64decode(b64str)
        else:
            raw = b64str
        return hashlib.sha1(raw).hexdigest()

    def _first_nonempty_image(self, product_data):
        """
        Prioriza image_1920 (plantilla) y usa image_variant_1920 si la de plantilla está vacía.
        """
        img = product_data.get('image_1920') or product_data.get('image_variant_1920')
        return img

    def _pick_best_image(self, data, fields_prefix='image_'):
        """
        Devuelve el mejor base64 disponible en orden:
        1920 > 1024 > 512 > 256 > 128
        data: dict de campos leídos vía XML-RPC.
        fields_prefix: 'image_' o 'image_variant_'.
        """
        for size in ('1920', '1024', '512', '256', '128'):
            key = f'{fields_prefix}{size}'
            val = data.get(key)
            if val:
                return val
        return None

    def _best_remote_image(self, p):
        """
        Busca primero en plantilla (image_*) y si no, en variante (image_variant_*).
        Escoge el mejor tamaño disponible.
        """
        img = self._pick_best_image(p, 'image_')
        if not img:
            img = self._pick_best_image(p, 'image_variant_')
        return img

# === STOCK: helpers de mapeo ===

    def _find_local_product_for_stock(self, p_tuple):
        """
        p_tuple viene de XML-RPC con formato (id, display_name) para product_id,
        pero nosotros además necesitamos default_code/barcode/name -> los pedimos aparte cuando haga falta.
        Aquí retornamos product.product (no template), priorizando:
        - default_code (en product.product)
        - barcode (en product.product)
        - name (en product.template)
        NOTA: si en tu base los códigos están en template, ajusta el search.
        """
        PP = self.env['product.product'].sudo()
        PT = self.env['product.template'].sudo()

        # Si tienes variantes 1:1, el name del template ayuda como último recurso.
        # Mejor: traeremos default_code/barcode por una cache adicional abajo.
        # Aquí dejamos un método neutro por name.
        name = p_tuple[1] if isinstance(p_tuple, (list, tuple)) and len(p_tuple) >= 2 else False
        if name:
            # intentamos por template name si variante única
            tmpl = PT.search([('name', '=', name)], limit=1)
            if tmpl and tmpl.product_variant_id:
                return tmpl.product_variant_id
        return PP.browse()  # vacío


    def _remote_fields(self, models, db, uid, password, model_name, candidates):
        """Devuelve solo los campos existentes en remoto."""
        try:
            fields = models.execute_kw(db, uid, password, model_name, 'fields_get', [[], ['string']])
            return [c for c in candidates if c in fields]
        except Exception:
            return []


    def _get_remote_internal_locations(self, models, db, uid, password):
        """Obtiene ubicaciones internas del remoto: id, complete_name, name."""
        loc_fields = self._remote_fields(models, db, uid, password, 'stock.location', ['id', 'complete_name', 'name', 'usage'])
        if not loc_fields:
            loc_fields = ['id', 'complete_name', 'name', 'usage']
        internal_ids = models.execute_kw(db, uid, password, 'stock.location', 'search', [[('usage', '=', 'internal')]])
        locs = models.execute_kw(db, uid, password, 'stock.location', 'read', [internal_ids, loc_fields])
        # Mapa por id remoto
        return {l['id']: l for l in locs}


    def _map_remote_location_to_local(self, remote_loc, by_complete_name=True):
        """
        Intenta encontrar la ubicación local equivalente:
        - por complete_name (recomendado) o por name.
        Si no existe, retorna el WH/Stock por defecto como último recurso.
        """
        SL = self.env['stock.location'].sudo()
        loc = False
        if by_complete_name and remote_loc.get('complete_name'):
            loc = SL.search([('complete_name', '=', remote_loc['complete_name'])], limit=1)
        if not loc and remote_loc.get('name'):
            loc = SL.search([('name', '=', remote_loc['name']), ('usage', '=', 'internal')], limit=1)
        if loc:
            return loc

        # fallback: Stock del almacén principal
        wh = self.env['stock.warehouse'].sudo().search([], limit=1)
        return wh.lot_stock_id if wh else SL.search([('usage', '=', 'internal')], limit=1)


    # === STOCK: proceso principal ===

    def migrate_stock_onhand(self, set_mode=True, commit_every=200):
        """
        Migra stock disponible (on-hand) desde Odoo 18 a la base local.
        - Lee stock.quant remoto en ubicaciones internas y agrega por (product_id, location_id).
        - Mapea ubicaciones por complete_name.
        - Ajusta inventario en local seteando 'inventory_quantity' y aplicando con 'action_apply_inventory()'.
        Params:
        set_mode(bool): True = pone el conteo exactamente igual al remoto (recomendado).
        commit_every(int): commit cada N líneas aplicadas.
        NOTAS:
        - Para productos con trazabilidad (lotes/series) este método ajusta el nivel global de la ubicación.
            Si requieres por lote/serie, necesitamos extender lectura a 'lot_id' y mapearlos.
        """
        uid, models = self.connect_to_odoo()

        # 1) Ubicaciones internas remotas
        remote_locs = self._get_remote_internal_locations(models, self.db, uid, self.password)
        remote_internal_ids = list(remote_locs.keys())
        if not remote_internal_ids:
            _logger.warning("[STOCK] No se encontraron ubicaciones internas en remoto.")
            return

        _logger.info(f"[STOCK] Ubicaciones internas remotas: {len(remote_internal_ids)}")

        # 2) Leer quants remotos (solo campos seguros)
        quant_fields = self._remote_fields(models, self.db, uid, self.password, 'stock.quant',
                                        ['id', 'product_id', 'location_id', 'quantity', 'reserved_quantity'])
        if not quant_fields:
            quant_fields = ['id', 'product_id', 'location_id', 'quantity', 'reserved_quantity']

        BATCH = 2000
        # Dominio: solo ubicaciones internas
        quant_ids = models.execute_kw(self.db, uid, self.password, 'stock.quant', 'search',
                                    [[('location_id', 'in', remote_internal_ids)]])
        _logger.info(f"[STOCK] Quants remotos a procesar: {len(quant_ids)}")

        # 3) Agregar cantidades por (product_id, location_id)
        from collections import defaultdict
        agg = defaultdict(float)

        for i in range(0, len(quant_ids), BATCH):
            batch = quant_ids[i:i+BATCH]
            quants = models.execute_kw(self.db, uid, self.password, 'stock.quant', 'read', [batch, quant_fields])
            for q in quants:
                qty = float(q.get('quantity') or 0.0)
                if not qty:
                    continue
                # product_id y location_id llegan como (id, display_name)
                p = q.get('product_id')
                l = q.get('location_id')
                if not p or not l:
                    continue
                key = (p[0], l[0])  # usar ids remotos para agregar
                agg[key] += qty

        _logger.info(f"[STOCK] Pares (producto, ubicación) agregados: {len(agg)}")

        # 4) Cache de mapeo producto remoto -> product.product local
        #    Traemos info extra de productos remotos: default_code, barcode, name, product_tmpl_id
        product_cache = {}
        product_fields = self._remote_fields(models, self.db, uid, self.password, 'product.product',
                                            ['id', 'default_code', 'barcode', 'name', 'product_tmpl_id'])
        if not product_fields:
            product_fields = ['id', 'default_code', 'barcode', 'name', 'product_tmpl_id']

        def get_local_product(remote_product_id):
            if remote_product_id in product_cache:
                return product_cache[remote_product_id]
            # leer el producto remoto
            pdata = models.execute_kw(self.db, uid, self.password, 'product.product', 'read',
                                    [[remote_product_id], product_fields])[0]
            PP = self.env['product.product'].sudo()
            PT = self.env['product.template'].sudo()

            # Prioridad: default_code -> barcode -> name (template)
            p_local = PP.search([('default_code', '=', pdata.get('default_code'))], limit=1) if pdata.get('default_code') else PP.browse()
            if not p_local and pdata.get('barcode'):
                p_local = PP.search([('barcode', '=', pdata.get('barcode'))], limit=1)
            if not p_local:
                # por name del template
                tmpl = pdata.get('product_tmpl_id')
                tmpl_name = tmpl[1] if isinstance(tmpl, (list, tuple)) and len(tmpl) >= 2 else pdata.get('name')
                if tmpl_name:
                    t = PT.search([('name', '=', tmpl_name)], limit=1)
                    if t and t.product_variant_id:
                        p_local = t.product_variant_id

            product_cache[remote_product_id] = p_local or PP.browse()
            return product_cache[remote_product_id]

        # 5) Aplicar inventario en local
        applied = 0
        errors = 0
        SL = self.env['stock.location'].sudo()
        SQ = self.env['stock.quant'].sudo()

        # Pre-cache de ubicaciones locales por remote_id (via complete_name)
        loc_cache = {}

        for (remote_pid, remote_lid), qty in agg.items():
            try:
                # producto local
                p_local = get_local_product(remote_pid)
                if not p_local or not p_local.id:
                    _logger.warning(f"[STOCK] Producto remoto {remote_pid} sin match local; se omite.")
                    continue

                # ubicación local
                if remote_lid in loc_cache:
                    loc_local = loc_cache[remote_lid]
                else:
                    loc_local = self._map_remote_location_to_local(remote_locs[remote_lid], by_complete_name=True)
                    loc_cache[remote_lid] = loc_local

                if not loc_local or not loc_local.id:
                    _logger.warning(f"[STOCK] Ubicación remota {remote_lid} sin mapeo local; se omite.")
                    continue

                # cuant local a ajustar
                quant = SQ.search([('product_id', '=', p_local.id), ('location_id', '=', loc_local.id)], limit=1)
                if not quant:
                    # crear un quant vacío (Odoo normalmente crea al aplicar inventario aunque no exista)
                    # Mejor usar el flujo soportado: setear inventory_quantity y aplicar
                    quant = SQ.create({
                        'product_id': p_local.id,
                        'location_id': loc_local.id,
                        'quantity': 0.0,
                    })

                # Modo "set": dejamos el stock contado exactamente igual al remoto
                # Para aplicar, se setea inventory_quantity y luego se llama a action_apply_inventory()
                quant.sudo().write({
                    'inventory_quantity': qty,
                })
                quant.sudo().action_apply_inventory()
                applied += 1

                if applied % commit_every == 0:
                    self.env.cr.commit()
                    _logger.info(f"[STOCK] Aplicados {applied} ajustes…")

            except Exception as e:
                errors += 1
                _logger.error(f"[STOCK] Error aplicando stock p={remote_pid}, l={remote_lid}: {e}")
                self.env.cr.rollback()

        _logger.info(f"[STOCK] Ajustes aplicados: {applied}, errores: {errors}")
        return True

    def _find_local_product_by_name(self, name, company_id=None):
        """
        Busca product.template por nombre de forma robusta:
        1) '=' exacto
        2) 'ilike'
        3) Normalizado (compara entre candidatos recientes)
        Retorna recordset (product.template) o vacío.
        """
        PT = self.env['product.template'].sudo()
        if not name:
            return PT.browse()

        dom_base = []
        if company_id:
            dom_base = ['|', ('company_id', '=', False), ('company_id', '=', company_id)]

        # 1) Igual exacto
        rec = PT.search(dom_base + [('name', '=', name)], limit=1)
        if rec:
            return rec

        # 2) ILIKE (más laxa)
        recs = PT.search(dom_base + [('name', 'ilike', name)], limit=5, order='id desc')
        if not recs:
            return PT.browse()

        # 3) Normalización para elegir el mejor
        target = self._norm_name(name)
        best = None
        for r in recs:
            if self._norm_name(r.name) == target:
                best = r
                break
        return best or recs[0]


# === Mapeo local: ya no depende de 'external_id'; si tienes x_external_id lo usa si existe ===
    def _find_local_product(self, product_data):
        """
        Mapea producto local por, en orden:
        - x_external_id (si existe el campo en local y viene en product_data)
        - default_code
        - barcode
        - name
        """
        PT = self.env['product.template'].sudo()

        # 1) x_external_id opcional (custom)
        if self._local_field_exists('product.template', 'x_external_id'):
            xext = product_data.get('x_external_id')  # solo existirá si lo pedimos al remoto
            if xext:
                rec = PT.search([('x_external_id', '=', xext)], limit=1)
                if rec:
                    return rec

        # 2) default_code
        default_code = product_data.get('default_code')
        if default_code:
            rec = PT.search([('default_code', '=', default_code)], limit=1)
            if rec:
                return rec

        # 3) barcode
        barcode = product_data.get('barcode')
        if barcode:
            rec = PT.search([('barcode', '=', barcode)], limit=1)
            if rec:
                return rec

        # 4) name
        name = product_data.get('name')
        if name:
            rec = PT.search([('name', '=', name)], limit=1)
            if rec:
                return rec

        return self.env['product.template']  # vacío


    def _remote_field_exists(self, models, db, uid, password, model_name, field_name):
        """Devuelve True si el campo existe en el modelo remoto."""
        try:
            fields = models.execute_kw(db, uid, password, model_name, 'fields_get', [[], ['string']])
            return field_name in fields
        except Exception:
            return False

    def _remote_fields(self, models, db, uid, password, model_name, candidates):
        """Filtra y devuelve solo los campos existentes en el remoto."""
        existing = []
        try:
            fields = models.execute_kw(db, uid, password, model_name, 'fields_get', [[], ['string']])
            for c in candidates:
                if c in fields:
                    existing.append(c)
        except Exception:
            # Si falla fields_get por alguna restricción, caemos a leer mínimos seguros
            pass
        return existing

    def _local_field_exists(self, model_name, field_name):
        """True si el campo existe en el modelo local (por ejemplo, x_external_id)."""
        return bool(self.env['ir.model.fields'].sudo().search([
            ('model', '=', model_name), ('name', '=', field_name)
        ], limit=1))

class ProductDataJSON(models.Model):
    _name = 'product.data.json'
    _description = 'Datos de productos en formato JSON'

    data = fields.Text('Datos de producto', required=True)

    @api.model
    def _get_local_publish_field(self):
        return 'is_published' if self.env['ir.model.fields'].sudo().search([
            ('model', '=', 'product.template'), ('name', '=', 'is_published')
        ], limit=1) else None

    @api.model
    def _find_local_category_id(self, remote_categ):
        if isinstance(remote_categ, (list, tuple)) and len(remote_categ) == 2:
            name = remote_categ[1]
            if name:
                cat = self.env['product.category'].sudo().search([('name', '=', name)], limit=1)
                return cat.id or False
        return False

    @api.model
    def process_stored_product_batches(self):
        batches = self.search([])
        publish_field = self._get_local_publish_field()

        for batch in batches:
            try:
                self.env.cr.commit()  # Guardar y reiniciar cursor entre lotes
                products = json.loads(batch.data)
                _logger.info(f"Procesando lote de {len(products)} productos")

                for i, p in enumerate(products):
                    try:
                        name = p.get('name')
                        if not name:
                            continue

                        domain = [('default_code', '=', p.get('default_code'))] if p.get('default_code') else [('name', '=', name)]
                        if self.env['product.template'].sudo().search(domain, limit=1):
                            continue

                        if p.get('barcode') and self.env['product.template'].sudo().search([('barcode', '=', p['barcode'])], limit=1):
                            p['barcode'] = False  # Elimina duplicado

                        remote_is_published = p.get('is_published')
                        if remote_is_published is None:
                            remote_is_published = p.get('website_published')

                        publish_field = self._get_local_publish_field()  # 'is_published' o None

                        vals = {
                            'name': name,
                            'default_code': p.get('default_code'),
                            'list_price': p.get('list_price', 0),
                            'standard_price': p.get('standard_price', 0),
                            'active': p.get('active', True),
                            'barcode': p.get('barcode'),
                        }
                        # flags si existen
                        if self.env['ir.model.fields'].sudo().search([('model','=','product.template'),('name','=','sale_ok')], limit=1):
                            vals['sale_ok'] = p.get('sale_ok', True)
                        if self.env['ir.model.fields'].sudo().search([('model','=','product.template'),('name','=','available_in_pos')], limit=1):
                            vals['available_in_pos'] = p.get('available_in_pos', False)
                        # publicación sólo is_published
                        if publish_field:
                            vals[publish_field] = bool(p.get('is_published', False))

                        categ_id = self._find_local_category_id(p.get('categ_id'))
                        if categ_id:
                            vals['categ_id'] = categ_id


                        categ_id = self._find_local_category_id(p.get('categ_id'))
                        if categ_id:
                            vals['categ_id'] = categ_id

                        self.env['product.template'].sudo().create(vals)

                        if (i + 1) % 10 == 0:
                            self.env.cr.commit()  # Comitea cada 10 para evitar cierre de cursor

                    except Exception as e:
                        _logger.error(f"Error con producto {p.get('name')}: {e}")
                        self.env.cr.rollback()

                batch.sudo().unlink()

            except Exception as e:
                _logger.error(f"Error al procesar lote: {e}")
                self.env.cr.rollback()

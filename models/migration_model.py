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
    # MIGRACIÓN DE CONTACTOS
    # =========================
    # =========================
    # CONTACTOS – Helpers
    # =========================
# ===== Helpers de matching por nombre (pegar dentro de ProductMigration) =====

    # --- Helpers faltantes / acteco ---
    # ===== Helpers robustos =====
    def _m2o_name(self, v):
        """Devuelve etiqueta de un many2one leído por XML-RPC: (id, name) -> name."""
        if isinstance(v, (list, tuple)):
            if len(v) >= 2 and isinstance(v[1], str):
                return v[1]
            if len(v) >= 1 and isinstance(v[0], str):
                return v[0]
            return ''
        return v or ''

    def _to_text(self, v):
        """Normaliza a texto para Char/Email/Phone, tolerando list/tuple/bytes/números/bool."""
        if v in (None, False):
            return ''
        if isinstance(v, (list, tuple)):
            return self._to_text(self._m2o_name(v))
        if isinstance(v, bytes):
            try:
                v = v.decode('utf-8', 'ignore')
            except Exception:
                v = str(v)
        elif isinstance(v, (int, float)):
            v = str(v)
        elif isinstance(v, bool):
            return ''
        return str(v).strip()

    def _norm_str(self, v):
        return self._to_text(v)

    def _norm_email(self, v):
        return self._to_text(v).lower()

    def _norm_name(self, s):
        s = self._to_text(s).lower()
        return ' '.join(s.split())  # colapsa espacios

    def _norm_rut(self, v):
        """
        Normaliza RUT: sin puntos/espacios/prefijo país; DV con guion.
        Ej: 'CL 76.624.4858' -> '76624485-8'
        """
        s = self._to_text(v).upper().replace('.', '').replace(' ', '')
        if not s:
            return ''
        # quita prefijo de país tipo CL / CHL / CL-:
        while s and s[0].isalpha():
            s = s[1:]
        if s.startswith('-'):
            s = s[1:]
        # si no trae guion y tiene largo > 1, separa DV:
        if '-' not in s and len(s) > 1:
            s = f"{s[:-1]}-{s[-1]}"
        return s

    # (por si no la tienes aún)
    def _remote_model_has(self, models, db, uid, password, model, field):
        try:
            fields = models.execute_kw(db, uid, password, model, 'fields_get', [[], ['string']])
            return field in fields
        except Exception:
            return False

    # (fallback mínimo para actecos si no lo tenías aún)
    def _map_remote_actecos_to_local(self, models, db, uid, password, remote_ids, create_missing=True):
        """
        Mapea l10n_cl.acteco (si existe). Busca por 'code' o 'name' remoto y devuelve ids locales.
        """
        res = []
        if not self._local_field_exists('res.partner', 'acteco_ids'):
            return res
        try:
            # lee code y name del remoto (modelo varía por módulo; usual: l10n_cl.acteco)
            model = 'l10n_cl.acteco'
            if not self._remote_model_has(models, db, uid, password, model, 'name'):
                return res
            recs = models.execute_kw(db, uid, password, model, 'read', [remote_ids, ['id', 'name', 'code']])
            L = self.env['l10n_cl.acteco'].sudo()
            for r in recs:
                code = (r.get('code') or '').strip()
                name = (r.get('name') or '').strip()
                dom = []
                if code:
                    dom = ['|', ('code', '=', code), ('name', '=', name)]
                elif name:
                    dom = [('name', '=', name)]
                act = L.search(dom, limit=1) if dom else L.browse()
                if not act and create_missing and name:
                    vals = {'name': name}
                    if 'code' in L._fields and code:
                        vals['code'] = code
                    act = L.create(vals)
                if act:
                    res.append(act.id)
        except Exception:
            pass
        return list(set(res))


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
    # ===== Migración de contactos robusta (Odoo 12 -> 16) =====
    def migrate_contacts(self, commit_every=10, force_overwrite=False, merge_actecos=True):
        uid, models = self.connect_to_odoo()

        has_acteco_local = self._local_field_exists('res.partner', 'acteco_ids')

        mapping, fields_to_read = self._build_remote_contact_fields(models, self.db, uid, self.password)
        # pide acteco_ids si existe en remoto
        if self._remote_model_has(models, self.db, uid, self.password, 'res.partner', 'acteco_ids'):
            if 'acteco_ids' not in mapping:
                mapping['acteco_ids'] = 'acteco_ids'
            if 'acteco_ids' not in fields_to_read:
                fields_to_read.append('acteco_ids')

        partner_ids = models.execute_kw(self.db, uid, self.password, 'res.partner', 'search', [[]])
        total = len(partner_ids)
        _logger.info(f"[PARTNER] Remotos a procesar: {total}")
        BATCH = 400

        Partner = self.env['res.partner'].sudo()

        # índices locales por RUT y email
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

        processed = created = updated = merged_act = replaced_act = 0

        for i in range(0, total, BATCH):
            batch_ids = partner_ids[i:i + BATCH]
            partners = models.execute_kw(self.db, uid, self.password, 'res.partner', 'read', [batch_ids, fields_to_read])

            for rp in partners:
                try:
                    def g(key, default=None):
                        f = mapping.get(key)
                        return rp.get(f) if f else default

                    name = self._norm_str(rp.get('name'))
                    vat  = self._norm_rut(g('vat'))
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

                    rc = g('country_id')
                    remote_country_name = self._m2o_name(rc)
                    rs = g('state_id')
                    remote_state_name = self._m2o_name(rs)

                    country_id = self._find_country_by_name(remote_country_name) if remote_country_name else False
                    state_id = self._find_state_by_name(country_id, remote_state_name) if (country_id and remote_state_name) else False
                    city = self._find_city_commune(city_in)

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

                    # acteco remoto -> ids locales
                    local_acteco_ids = []
                    if has_acteco_local and 'acteco_ids' in mapping:
                        raw = g('acteco_ids') or []
                        if isinstance(raw, list) and raw and isinstance(raw[0], int):
                            local_acteco_ids = self._map_remote_actecos_to_local(models, self.db, uid, self.password, raw, create_missing=True)

                    # localizar/crear
                    local = _find_local({'vat': vat, 'email': email, 'name': name, 'city': city})

                    incoming = {
                        'is_company': is_company,
                        'name': name or email or vat or 'Sin Nombre',
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

                    if local and local.id:
                        vals_write = incoming if force_overwrite else self._merge_fill_missing(local, incoming)
                        if vals_write:
                            local.write(vals_write)
                            updated += 1
                            _logger.info(f"[PARTNER] Actualizado: {local.display_name} -> {list(vals_write.keys())}")
                        if has_acteco_local and local_acteco_ids:
                            if merge_actecos:
                                new_set = list(sorted(set(local.acteco_ids.ids).union(local_acteco_ids)))
                                if set(new_set) != set(local.acteco_ids.ids):
                                    local.write({'acteco_ids': [(6, 0, new_set)]})
                                    merged_act += 1
                            else:
                                local.write({'acteco_ids': [(6, 0, local_acteco_ids)]})
                                replaced_act += 1
                    else:
                        local = Partner.create(incoming)
                        created += 1
                        _logger.info(f"[PARTNER] Creado: {local.display_name}")
                        if has_acteco_local and local_acteco_ids:
                            local.write({'acteco_ids': [(6, 0, local_acteco_ids)]})
                            replaced_act += 1

                    processed += 1
                    if processed % commit_every == 0:
                        self.env.cr.commit()
                        _logger.info(f"[PARTNER] Parcial: proc={processed}/{total}, creados={created}, actualizados={updated}, acteco_merge={merged_act}, acteco_set={replaced_act}")

                except Exception as e:
                    self.env.cr.rollback()
                    _logger.error(f"[PARTNER] Error migrando '{self._to_text(rp.get('name'))}' : {e}")

        self.env.cr.commit()
        _logger.info(f"[PARTNER] FIN — proc={processed}, creados={created}, actualizados={updated}, acteco_merge={merged_act}, acteco_set={replaced_act}")
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
    def migrate_product_images(self, limit=None, commit_every=50):
        """
        Migra imágenes desde Odoo remoto (12–17) hacia local (13+).
        Fuente: product.product (image_variant / image)
        Destino: product.template.image_1920
        """

        uid, models = self.connect_to_odoo()

        # 1) IDs de variantes remotas
        remote_pp_ids = self.execute_kw_with_retry(
            models, self.db, uid, self.password,
            'product.product', 'search', [[]]
        )
        if limit:
            remote_pp_ids = remote_pp_ids[:limit]

        _logger.info("[IMG] Variantes remotas a procesar: %s", len(remote_pp_ids))

        updated = skipped = nomatch = noimg = errors = 0

        # 2) Campos reales Odoo 12
        fields = ['id', 'product_tmpl_id', 'image_variant', 'image', 'default_code', 'barcode']

        for i in range(0, len(remote_pp_ids), 100):
            batch = remote_pp_ids[i:i + 100]

            products = self.execute_kw_with_retry(
                models, self.db, uid, self.password,
                'product.product', 'read',
                [batch, fields],
                {'context': {'bin_size': False}}  # 🔴 CLAVE
            )

            for p in products:
                try:
                    # 3) Tomar imagen real
                    img = p.get('image_variant') or p.get('image')
                    if not img or isinstance(img, int):
                        noimg += 1
                        continue

                    # 4) Resolver template remoto
                    tmpl = p.get('product_tmpl_id')
                    if not tmpl:
                        nomatch += 1
                        continue

                    tmpl_id = tmpl[0]

                    # 5) Buscar template local (orden estable)
                    local_tmpl = False
                    if p.get('default_code'):
                        local_tmpl = self.env['product.template'].search(
                            [('default_code', '=', p['default_code'])], limit=1
                        )
                    if not local_tmpl and p.get('barcode'):
                        local_tmpl = self.env['product.template'].search(
                            [('barcode', '=', p['barcode'])], limit=1
                        )
                    if not local_tmpl:
                        local_tmpl = self.env['product.template'].search(
                            [('name', '=', tmpl[1])], limit=1
                        )

                    if not local_tmpl:
                        nomatch += 1
                        continue

                    # 6) No pisar si ya tiene imagen
                    if local_tmpl.image_1920:
                        skipped += 1
                        continue

                    # 7) Escritura canónica
                    local_tmpl.sudo().write({
                        'image_1920': img
                    })
                    updated += 1

                    if updated % commit_every == 0:
                        self.env.cr.commit()
                        _logger.info(
                            "[IMG] Parcial updated=%s skipped=%s noimg=%s nomatch=%s errors=%s",
                            updated, skipped, noimg, nomatch, errors
                        )

                except Exception as e:
                    errors += 1
                    self.env.cr.rollback()
                    _logger.error("[IMG] Error procesando pp=%s: %s", p.get('id'), e)

        self.env.cr.commit()
        _logger.info(
            "[IMG] FIN updated=%s skipped=%s noimg=%s nomatch=%s errors=%s",
            updated, skipped, noimg, nomatch, errors
        )
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

    # =========================
    # STOCK (Odoo 12 -> 16)
    # =========================

    def _safe_m2o_name(self, v):
        if isinstance(v, (list, tuple)):
            if len(v) >= 2 and isinstance(v[1], str):
                return v[1]
            if len(v) >= 1:
                return str(v[0])
        return (v or '') if isinstance(v, str) else ''

    def _remote_fields(self, models, db, uid, password, model_name, candidates):
        """Devuelve solo los campos existentes en remoto (v12)."""
        try:
            fields = models.execute_kw(db, uid, password, model_name, 'fields_get', [[], ['string']])
            return [c for c in candidates if c in fields]
        except Exception:
            return []

    def _get_remote_internal_locations(self, models, db, uid, password):
        """Ubicaciones internas remotas (id, complete_name, name, usage)."""
        loc_fields = self._remote_fields(models, db, uid, password, 'stock.location',
                                        ['id', 'complete_name', 'name', 'usage'])
        if not loc_fields:
            loc_fields = ['id', 'complete_name', 'name', 'usage']
        internal_ids = models.execute_kw(db, uid, password, 'stock.location', 'search',
                                        [[('usage', '=', 'internal')]], {'context': {'active_test': False}})
        locs = models.execute_kw(db, uid, password, 'stock.location', 'read',
                                [internal_ids, loc_fields])
        return {l['id']: l for l in locs}

    def _map_remote_location_to_local(self, remote_loc, by_complete_name=True):
        """
        Mapea ubicación remota -> local:
        - primero por complete_name
        - luego por name (solo internas)
        - fallback: WH principal / cualquier interna
        """
        SL = self.env['stock.location'].sudo()
        loc = False
        if by_complete_name and remote_loc.get('complete_name'):
            loc = SL.search([('complete_name', '=', remote_loc['complete_name'])], limit=1)
        if not loc and remote_loc.get('name'):
            loc = SL.search([('name', '=', remote_loc['name']), ('usage', '=', 'internal')], limit=1)
        if loc:
            return loc
        wh = self.env['stock.warehouse'].sudo().search([], limit=1)
        return wh.lot_stock_id if wh else SL.search([('usage', '=', 'internal')], limit=1)

    def _read_remote_product_pp_pt(self, models, db, uid, password, remote_product_id, cache,
                                pp_fields, pt_fields):
        """
        Lee product.product remoto (v12). Si falta default_code/barcode en pp, intenta leerlos del template.
        Cachea por performance.
        """
        if remote_product_id in cache:
            return cache[remote_product_id]

        pdata = models.execute_kw(db, uid, password, 'product.product', 'read',
                                [[remote_product_id], pp_fields])[0]
        tmpl = pdata.get('product_tmpl_id')
        tmpl_id = tmpl[0] if isinstance(tmpl, (list, tuple)) and tmpl else False
        tdata = {}
        if tmpl_id:
            tdata = models.execute_kw(db, uid, password, 'product.template', 'read',
                                    [[tmpl_id], pt_fields])[0]

        # Consolidar códigos posibles (en v12 a veces están en el template)
        default_code = pdata.get('default_code') or tdata.get('default_code') or False
        barcode = pdata.get('barcode') or tdata.get('barcode') or False
        tmpl_name = tmpl[1] if isinstance(tmpl, (list, tuple)) and len(tmpl) >= 2 else tdata.get('name') or pdata.get('name')

        out = {
            'pp_id': remote_product_id,
            'pp_name': pdata.get('name'),
            'pp_default_code': pdata.get('default_code'),
            'pp_barcode': pdata.get('barcode'),
            'pt_id': tmpl_id,
            'pt_name': tmpl_name,
            'pt_default_code': tdata.get('default_code'),
            'pt_barcode': tdata.get('barcode'),
            'default_code': default_code,
            'barcode': barcode,
        }
        cache[remote_product_id] = out
        return out

    def _map_remote_product_to_local(self, models, db, uid, password, remote_product_id, cache):
        """
        Mapea un product.product remoto (v12) a product.product local (v16):
        Prioridad: product.product.default_code -> product.product.barcode
                -> product.template.default_code -> product.template.name (y variante única)
        """
        PP = self.env['product.product'].sudo()
        PT = self.env['product.template'].sudo()

        pp_fields = self._remote_fields(models, db, uid, password, 'product.product',
                                        ['id', 'name', 'default_code', 'barcode', 'product_tmpl_id'])
        if not pp_fields:
            pp_fields = ['id', 'name', 'default_code', 'barcode', 'product_tmpl_id']
        pt_fields = self._remote_fields(models, db, uid, password, 'product.template',
                                        ['id', 'name', 'default_code', 'barcode'])
        if not pt_fields:
            pt_fields = ['id', 'name', 'default_code', 'barcode']

        info = self._read_remote_product_pp_pt(models, db, uid, password,
                                            remote_product_id, cache, pp_fields, pt_fields)
        # 1) product.product por default_code
        if info['default_code']:
            p = PP.search([('default_code', '=', info['default_code'])], limit=1)
            if p:
                return p
        # 2) product.product por barcode
        if info['barcode']:
            p = PP.search([('barcode', '=', info['barcode'])], limit=1)
            if p:
                return p
        # 3) template por default_code
        if info['pt_default_code']:
            t = PT.search([('default_code', '=', info['pt_default_code'])], limit=1)
            if t and t.product_variant_id:
                return t.product_variant_id
        # 4) template por nombre (si es variante única)
        if info['pt_name']:
            t = PT.search([('name', '=', info['pt_name'])], limit=1)
            if t and t.product_variant_id:
                return t.product_variant_id
        return PP.browse()  # vacío

    def migrate_stock_onhand(self, set_mode=True, commit_every=200, include_zero=False):
        """
        Migra stock on-hand desde **Odoo 12 (remoto)** hacia **Odoo 16 (local)**.

        - Lee stock.quant remoto, suma por (product_id, location_id) sólo ubicaciones internas.
        - Mapea producto remoto -> product.product local (ver prioridades).
        - Mapea ubicación remota -> local por complete_name / name (fallback a WH/Stock).
        - Si set_mode=True: fija el stock contado (= remoto) usando inventory_quantity + action_apply_inventory().
        (Esto reemplaza el nivel existente).
        - commit_every: commits periódicos.
        - include_zero: si True, también genera ajustes para poner en 0 donde corresponda.
        """
        uid, models = self.connect_to_odoo()

        # 1) ubicaciones remotas
        remote_locs = self._get_remote_internal_locations(models, self.db, uid, self.password)
        remote_internal_ids = list(remote_locs.keys())
        if not remote_internal_ids:
            _logger.warning("[STOCK] No se encontraron ubicaciones internas en el remoto.")
            return False
        _logger.info(f"[STOCK] Ubicaciones internas remotas: {len(remote_internal_ids)}")

        # 2) quants remotos (en v12 los campos usuales: product_id, location_id, quantity)
        quant_fields = self._remote_fields(models, self.db, uid, self.password, 'stock.quant',
                                        ['id', 'product_id', 'location_id', 'quantity'])
        if not quant_fields:
            quant_fields = ['id', 'product_id', 'location_id', 'quantity']

        # Dominio: sólo ubicaciones internas
        quant_ids = models.execute_kw(self.db, uid, self.password, 'stock.quant', 'search',
                                    [[('location_id', 'in', remote_internal_ids)]],
                                    {'context': {'active_test': False}})
        _logger.info(f"[STOCK] Quants remotos a procesar: {len(quant_ids)}")

        # 3) Agregar cantidades por (product_id, location_id)
        from collections import defaultdict
        agg = defaultdict(float)
        BATCH = 2000
        for i in range(0, len(quant_ids), BATCH):
            batch = quant_ids[i:i + BATCH]
            quants = models.execute_kw(self.db, uid, self.password, 'stock.quant', 'read',
                                    [batch, quant_fields])
            for q in quants:
                qty = float(q.get('quantity') or 0.0)
                if not include_zero and not qty:
                    continue
                p = q.get('product_id')
                l = q.get('location_id')
                if not p or not l:
                    continue
                key = (p[0], l[0])  # IDs remotos
                agg[key] += qty

        _logger.info(f"[STOCK] Pares agregados (producto, ubicación): {len(agg)}")

        # 4) Cache para mapear producto remoto -> local
        pp_cache = {}

        applied = 0
        errors = 0
        SQ = self.env['stock.quant'].sudo()
        loc_cache = {}

        # Para mejorar auditoría de inventario
        ctx_inv = dict(self.env.context or {})
        # Estos context no son obligatorios, pero ayudan a trazar
        ctx_inv.update({
            'inventory_name': 'Migración Odoo12',
            'inventory_reason': 'migration',
            'tracking_disable': True,
            'mail_notrack': True,
        })

        for (remote_pid, remote_lid), qty in agg.items():
            try:
                # producto local
                p_local = self._map_remote_product_to_local(models, self.db, uid, self.password,
                                                            remote_pid, pp_cache)
                if not p_local or not p_local.id:
                    _logger.warning(f"[STOCK] Producto remoto {remote_pid} sin match local; skip.")
                    continue

                # ubicación local
                if remote_lid in loc_cache:
                    loc_local = loc_cache[remote_lid]
                else:
                    loc_local = self._map_remote_location_to_local(remote_locs[remote_lid], by_complete_name=True)
                    loc_cache[remote_lid] = loc_local

                if not loc_local or not loc_local.id:
                    _logger.warning(f"[STOCK] Ubicación remota {remote_lid} sin mapeo local; skip.")
                    continue

                # quant local
                quant = SQ.search([('product_id', '=', p_local.id),
                                ('location_id', '=', loc_local.id)], limit=1)
                if not quant:
                    # Crear quant 0 si no existe; v16 permite crear quant directo
                    quant = SQ.create({
                        'product_id': p_local.id,
                        'location_id': loc_local.id,
                        'quantity': 0.0,
                    })

                # set_mode => fijar exactamente el conteo al remoto
                if set_mode:
                    quant.with_context(ctx_inv).sudo().write({'inventory_quantity': qty})
                    quant.with_context(ctx_inv).sudo().action_apply_inventory()
                else:
                    # modo delta (opcional): ajusta a qty sin pisar; típico sería calcular delta y usar move,
                    # pero para simplicidad mantenemos apply_inventory igual.
                    quant.with_context(ctx_inv).sudo().write({'inventory_quantity': qty})
                    quant.with_context(ctx_inv).sudo().action_apply_inventory()

                applied += 1
                if applied % commit_every == 0:
                    self.env.cr.commit()
                    _logger.info(f"[STOCK] Aplicados {applied} ajustes de inventario…")

            except Exception as e:
                errors += 1
                self.env.cr.rollback()
                _logger.error(f"[STOCK] Error aplicando p={remote_pid}, l={remote_lid}, qty={qty}: {e}")

        self.env.cr.commit()
        _logger.info(f"[STOCK] FIN — Ajustes aplicados: {applied}, errores: {errors}")
        return True

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
    def process_stored_product_batches(self, commit_every=200, dry_run=False, target_company_id=False):
        """
        Procesa todos los registros product.data.json y **solo crea productos nuevos**.
        - Match de existencia por prioridad: default_code -> barcode -> name
        - No actualiza existentes.
        - Evita 'website_published'; sólo usa 'is_published' si existe localmente.
        - Hace commit cada `commit_every` creaciones para evitar timeouts.
        - Si `dry_run=True`, no crea ni borra lotes: sólo log.
        - Si `target_company_id` se pasa, crea asignando esa compañía cuando aplica.
        """
        PT = self.env['product.template'].sudo()
        publish_field = self._get_local_publish_field()

        batches = self.search([])
        if not batches:
            _logger.info("[JSON] No hay lotes pendientes.")
            return True

        # Precache de existentes para acelerar (por code/barcode/name)
        def _index_existentes():
            # Traemos sólo lo necesario
            fields = ['id', 'name', 'default_code', 'barcode']
            dom = []
            if target_company_id:
                dom = ['|', ('company_id', '=', False), ('company_id', '=', target_company_id)]
            recs = PT.search(dom)
            codes = set(filter(None, recs.mapped('default_code')))
            barcs = set(filter(None, recs.mapped('barcode')))
            names = set(filter(None, recs.mapped('name')))
            return codes, barcs, names

        codes_idx, barcodes_idx, names_idx = _index_existentes()

        def _exists_locally(p):
            dc = (p.get('default_code') or '').strip()
            if dc and dc in codes_idx:
                return True
            bc = (p.get('barcode') or '').strip()
            if bc and bc in barcodes_idx:
                return True
            nm = (p.get('name') or '').strip()
            if nm and nm in names_idx:
                return True
            return False

        created_total = 0
        skipped_existing = 0
        errors = 0

        for batch in batches:
            try:
                products = json.loads(batch.data or '[]')
            except Exception as e:
                _logger.error(f"[JSON] Lote inválido (id={batch.id}): {e}")
                continue

            if not products:
                _logger.info(f"[JSON] Lote vacío (id={batch.id}), se elimina.")
                if not dry_run:
                    batch.sudo().unlink()
                continue

            _logger.info(f"[JSON] Lote {batch.id}: {len(products)} productos a evaluar (solo-nuevos).")

            to_create = []
            for p in products:
                try:
                    # Saltar si ya existe por default_code/barcode/name
                    if _exists_locally(p):
                        skipped_existing += 1
                        continue

                    # Construir vals de forma segura
                    vals = {
                        'name': p.get('name'),
                        'default_code': p.get('default_code') or False,
                        'list_price': p.get('list_price', 0.0),
                        'standard_price': p.get('standard_price', 0.0),
                        'active': bool(p.get('active', True)),
                        # NO poner barcode aquí si ya lo tiene otro producto,
                        # lo verificamos antes de asignarlo abajo.
                    }

                    # compañía destino explícita (si corresponde)
                    if target_company_id:
                        vals['company_id'] = target_company_id

                    # flags si existen en esta base
                    if self.env['ir.model.fields'].sudo().search(
                        [('model','=','product.template'),('name','=','sale_ok')], limit=1):
                        vals['sale_ok'] = bool(p.get('sale_ok', True))
                    if self.env['ir.model.fields'].sudo().search(
                        [('model','=','product.template'),('name','=','available_in_pos')], limit=1):
                        vals['available_in_pos'] = bool(p.get('available_in_pos', False))
                    if publish_field:
                        # NO usamos website_published en 16; sólo is_published si existe
                        vals[publish_field] = bool(p.get('is_published', False))

                    # categoría local por nombre
                    categ_id = self._find_local_category_id(p.get('categ_id'))
                    if categ_id:
                        vals['categ_id'] = categ_id

                    # barcode: sólo si no existe ya en local
                    barcode = (p.get('barcode') or '').strip()
                    if barcode and barcode not in barcodes_idx:
                        vals['barcode'] = barcode

                    to_create.append(vals)

                except Exception as e:
                    errors += 1
                    _logger.error(f"[JSON] Error preparando '{p.get('name')}': {e}")

            # Crear en bloques
            if to_create:
                _logger.info(f"[JSON] Lote {batch.id}: {len(to_create)} productos nuevos serán creados.")
                if not dry_run:
                    # creación controlada por chunks
                    chunk = []
                    for idx, vals in enumerate(to_create, 1):
                        try:
                            rec = PT.create(vals)
                            created_total += 1

                            # actualizar índices in-memory para evitar duplicados en el mismo lote
                            if rec.default_code:
                                codes_idx.add(rec.default_code)
                            if rec.barcode:
                                barcodes_idx.add(rec.barcode)
                            if rec.name:
                                names_idx.add(rec.name)

                            chunk.append(rec.id)
                            if created_total % commit_every == 0:
                                self.env.cr.commit()
                                _logger.info(f"[JSON] Commit de seguridad: {created_total} creados.")

                        except Exception as e:
                            errors += 1
                            self.env.cr.rollback()
                            _logger.error(f"[JSON] Error creando '{vals.get('name')}': {e}")

            else:
                _logger.info(f"[JSON] Lote {batch.id}: no hay productos nuevos (todo ya existe).")

            # limpiar lote procesado
            if not dry_run:
                try:
                    batch.sudo().unlink()
                except Exception as e:
                    _logger.warning(f"[JSON] No se pudo remover el lote {batch.id}: {e}")

        # commit final
        if not dry_run:
            self.env.cr.commit()

        _logger.info(f"[JSON] FIN: creados={created_total}, existentes_saltados={skipped_existing}, errores={errors}")
        return True

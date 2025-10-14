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
        """Devuelve 'is_published' si existe en local; de lo contrario 'website_published'."""
        field_model = self.env['ir.model.fields'].sudo()
        if field_model.search([('model', '=', 'product.template'), ('name', '=', 'is_published')], limit=1):
            return 'is_published'
        return 'website_published'

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
        Construye el diccionario de creación/actualización local incorporando:
        - categoría (categ_id)
        - referencia (default_code)
        - e-commerce publicado (is_published/website_published)
        - vendible (sale_ok)
        - disponible en POS (available_in_pos)
        """
        publish_field = self._get_local_publish_field()

        # Compatibilidad con remoto Odoo 18/16/15
        remote_is_published = product_data.get('is_published')
        if remote_is_published is None:
            remote_is_published = product_data.get('website_published')

        vals = {
            'name': product_data.get('name'),
            'default_code': product_data.get('default_code'),
            'list_price': product_data.get('list_price'),
            'standard_price': product_data.get('standard_price'),
            'active': product_data.get('active'),
            'barcode': product_data.get('barcode'),
            'sale_ok': product_data.get('sale_ok', True),
            'available_in_pos': product_data.get('available_in_pos', False),
            publish_field: bool(remote_is_published),
        }

        categ_id = self._find_local_category_id(product_data.get('categ_id'))
        if categ_id:
            vals['categ_id'] = categ_id

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
    def migrate_contacts(self):
        uid, models = self.connect_to_odoo()
        contacts = models.execute_kw(
            self.db, uid, self.password,
            'res.partner', 'search_read',
            [[]],
            {'fields': ['is_company', 'name', 'street', 'city', 'state_id', 'country_id',
                        'vat', 'function', 'phone', 'email', 'l10n_cl_dte_email']}
        )

        for contact in contacts:
            try:
                local_contact_vals = {
                    'is_company': contact.get('is_company'),
                    'name': contact.get('name'),
                    'street': contact.get('street') or '',
                    'document_number': contact.get('vat', ''),  # si existe en tu modelo local
                    'phone': contact.get('phone', ''),
                    'email': contact.get('email', ''),
                    'dte_email': contact.get('l10n_cl_dte_email', ''),
                }

                existing_contact = self.env['res.partner'].search(
                    [('name', '=', contact.get('name')), ('vat', '=', contact.get('vat', ''))],
                    limit=1
                )

                if existing_contact:
                    existing_contact.write(local_contact_vals)
                    _logger.info(f"Contacto actualizado: {contact.get('name')}")
                else:
                    self.env['res.partner'].create(local_contact_vals)
                    _logger.info(f"Contacto creado: {contact.get('name')}")

            except Exception as e:
                _logger.error(f"Error al migrar el contacto {contact.get('name')}: {e}")

        _logger.info("Migración de contactos completada.")

    # =========================
    # MIGRACIÓN DE PRODUCTOS (Odoo 18 -> local)
    # =========================
    def migrate_products(self):
        """
        Migra productos activos desde Odoo 18 (remoto) a Odoo 16/15 (local),
        incluyendo categoría, referencia, flags de e-commerce y POS.
        Omite productos con códigos/Barcodes existentes.
        """
        BATCH_SIZE = 100
        uid, models = self.connect_to_odoo()
        barcodes_seen = set()

        try:
            total_products = models.execute_kw(
                self.db, uid, self.password,
                'product.template', 'search_count',
                [[('active', '=', True)]]
            )
            _logger.info('Total de productos activos a migrar: %s', total_products)

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

                for product_data in product_batch:
                    try:
                        default_code = product_data.get('default_code')
                        name = product_data.get('name')
                        barcode = product_data.get('barcode')

                        domain = [('default_code', '=', default_code)] if default_code else [('name', '=', name)]
                        existing_product = self.env['product.template'].sudo().search(domain, limit=1)
                        if existing_product:
                            _logger.info(f"Producto ya existe localmente: {existing_product.name}, se omite.")
                            continue

                        if barcode:
                            if barcode in barcodes_seen:
                                _logger.warning(f"Barcode duplicado ya visto en esta sesión: {barcode}, se omite.")
                                continue
                            if self.env['product.template'].sudo().search([('barcode', '=', barcode)], limit=1):
                                _logger.warning(f"Barcode ya existe en local: {barcode}, se omite.")
                                continue
                            barcodes_seen.add(barcode)

                        product_vals = self._build_product_vals(product_data)
                        new_product = self.env['product.template'].sudo().create(product_vals)
                        _logger.info(f"Producto creado: {new_product.name} (ID {new_product.id})")

                    except Exception as e:
                        _logger.error(f"Error al procesar producto {product_data.get('name')}: {e}")

        except Exception as e:
            _logger.error(f"Error general en la migración de productos: {e}")
            raise UserError(f"Error general en la migración: {str(e)}")

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
    def migrate_product_images(self, force=False, prefer_variant=False):
        """
        Copia las imágenes EXACTAS desde Odoo remoto a local (sin conversiones).
        - Toma image_1920 directamente del remoto y la asigna a image_1920 local.
        - Si prefer_variant=True y la variante tiene imagen, usa image_variant_1920 primero.
        - No recomprime, no convierte; pasa el base64 tal cual.
        - Si force=False y ya hay imagen local igual, no reescribe.

        Params:
            force(bool): sobrescribir aunque ya tenga imagen local.
            prefer_variant(bool): prioriza image_variant_1920 sobre image_1920 de plantilla.
        """
        uid, models = self.connect_to_odoo()

        # 1) Traer todos los productos activos (filtramos luego por los que sí tengan imagen)
        try:
            remote_ids = self.execute_kw_with_retry(
                models, self.db, uid, self.password,
                'product.template', 'search',
                [[('active', '=', True)]],
            )
        except Exception as e:
            _logger.error(f"No se pudieron obtener IDs remotos: {e}")
            return

        _logger.info(f"[IMÁGENES EXACTAS] Productos remotos a revisar: {len(remote_ids)}")

        BATCH = 50
        fields_to_read = [
            'id', 'name', 'external_id', 'default_code', 'barcode',
            'image_1920', 'image_variant_1920',
        ]

        # Pequeño helper local para comparar hashes sin tocar PIL
        import hashlib, base64
        def _sha1_b64(b64str):
            if not b64str:
                return None
            try:
                raw = base64.b64decode(b64str)
            except Exception:
                # si ya viniera bytes o algo raro, lo tratamos directo
                raw = b64str if isinstance(b64str, (bytes, bytearray)) else bytes(str(b64str), 'utf-8')
            return hashlib.sha1(raw).hexdigest()

        for i in range(0, len(remote_ids), BATCH):
            batch_ids = remote_ids[i:i + BATCH]
            try:
                products = self.execute_kw_with_retry(
                    models, self.db, uid, self.password,
                    'product.template', 'read',
                    [batch_ids, fields_to_read],
                )
            except Exception as e:
                _logger.error(f"Fallo leyendo batch {i}-{i+BATCH}: {e}")
                continue

            for p in products:
                try:
                    # 2) Elegir fuente exacta según prefer_variant
                    img_b64 = None
                    if prefer_variant:
                        img_b64 = p.get('image_variant_1920') or p.get('image_1920')
                    else:
                        img_b64 = p.get('image_1920') or p.get('image_variant_1920')

                    if not img_b64:
                        # No hay imagen en el remoto
                        continue

                    # 3) Buscar el producto local
                    local_prod = self._find_local_product(p)
                    if not local_prod or not local_prod.id:
                        _logger.warning(f"[IMÁGENES EXACTAS] Sin match local para '{p.get('name')}' (remoto {p.get('id')}).")
                        continue

                    # 4) Si ya tiene imagen y no forzamos, comparamos hash para evitar trabajo
                    if local_prod.image_1920 and not force:
                        if _sha1_b64(local_prod.image_1920) == _sha1_b64(img_b64):
                            continue  # es exactamente la misma imagen

                    # 5) Escribir TAL CUAL en image_1920; limpiamos miniaturas para que se recalculen
                    vals = {
                        'image_128': False,
                        'image_256': False,
                        'image_512': False,
                        'image_1024': False,
                        'image_1920': img_b64,  # exacto
                    }
                    local_prod.sudo().write(vals)
                    _logger.info(f"[IMÁGENES EXACTAS] Imagen copiada en '{local_prod.display_name}' (force={force}, prefer_variant={prefer_variant}).")

                except Exception as e:
                    _logger.error(f"[IMÁGENES EXACTAS] Error migrando imagen de '{p.get('name')}' (id remoto {p.get('id')}): {e}")

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


    def _find_local_product(self, product_data):
        """
        Mapear producto local por: external_id > default_code > barcode > name.
        """
        PT = self.env['product.template'].sudo()

        ext_id = product_data.get('external_id')
        if ext_id:
            rec = PT.search([('external_id', '=', ext_id)], limit=1)
            if rec:
                return rec

        default_code = product_data.get('default_code')
        if default_code:
            rec = PT.search([('default_code', '=', default_code)], limit=1)
            if rec:
                return rec

        barcode = product_data.get('barcode')
        if barcode:
            rec = PT.search([('barcode', '=', barcode)], limit=1)
            if rec:
                return rec

        name = product_data.get('name')
        if name:
            rec = PT.search([('name', '=', name)], limit=1)
            if rec:
                return rec

        return self.env['product.template']  # vacío


class ProductDataJSON(models.Model):
    _name = 'product.data.json'
    _description = 'Datos de productos en formato JSON'

    data = fields.Text('Datos de producto', required=True)

    @api.model
    def _get_local_publish_field(self):
        field_model = self.env['ir.model.fields'].sudo()
        if field_model.search([('model', '=', 'product.template'), ('name', '=', 'is_published')], limit=1):
            return 'is_published'
        return 'website_published'

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

                        vals = {
                            'name': name,
                            'default_code': p.get('default_code'),
                            'list_price': p.get('list_price', 0),
                            'standard_price': p.get('standard_price', 0),
                            'active': p.get('active', True),
                            'barcode': p.get('barcode'),
                            'sale_ok': p.get('sale_ok', True),
                            'available_in_pos': p.get('available_in_pos', False),
                            publish_field: bool(remote_is_published),
                        }

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

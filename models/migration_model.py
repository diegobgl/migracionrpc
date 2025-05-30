# -*- coding: utf-8 -*-
import base64
import logging
from odoo import models, api, fields
import tempfile
import xmlrpc.client  # Importa el módulo xmlrpc.client
import json
import time
import psycopg2
import logging


_logger = logging.getLogger(__name__)



class ProductMigration(models.Model):
    _name = 'product.migration'
    _description = 'Migración de Productos'

    url = fields.Char(string="url")
    db = fields.Char(string="Base de Datos")
    username = fields.Char(string="Usuario")
    password = fields.Char(string="Contraseña")

    def connect_to_odoo(self):
        try:
            common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(self.url))
            uid = common.authenticate(self.db, self.username, self.password, {})
            models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self.url))
            return uid, models
        except Exception as e:
            _logger.error('Error al conectar a Odoo : %s', e)
            raise

    def execute_kw_with_retry(self, models, db, uid, password, model, method, args, kwargs={}):
        max_retries = 5
        wait_seconds = 5
        for attempt in range(max_retries):
            try:
                return models.execute_kw(db, uid, password, model, method, args, kwargs)
            except xmlrpc.client.ProtocolError as e:
                if e.errcode == 429:  # Too Many Requests
                    _logger.warning(f"429 Too Many Requests, esperando {wait_seconds} segundos antes de reintentar...")
                    time.sleep(wait_seconds) 
                    wait_seconds *= 2  
                else:
                    raise e
        raise Exception("Max retries reached for execute_kw_with_retry")

    def copiar_imagenes_productos(self):
        """
        Copia las imágenes de productos de tamaño 128x128 de Odoo 17 a la base de datos local de Odoo 15.

        Parámetros:
            - Ninguno

        Devuelve:
            - Ninguno
        """

        uid, models = self.connect_to_odoo()

        try:
            productos_remoto = self.execute_kw_with_retry(models, self.db, uid, self.password, 'product.template', 'search', [[('name', '!=', False)]])
            _logger.info(f"Total de productos con imágenes para migrar: {len(productos_remoto)}")

            for product_id in productos_remoto:
                if product_id > 0:
                    # Ahora obtenemos el campo image_128 junto con el nombre del producto
                    datos_imagen = self.execute_kw_with_retry(models, self.db, uid, self.password, 'product.template', 'read', [product_id, ['image_128', 'name']])
                    if datos_imagen and datos_imagen[0]['image_128']:
                        producto_local = self.env['product.template'].search([('name', '=', datos_imagen[0]['name'])], limit=1)
                        if producto_local and not producto_local.image_128:
                            # Aquí, estamos seguros de que 'datos_imagen' está definido y contiene la imagen de tamaño adecuado.
                            producto_local.image_128 = datos_imagen[0]['image_128']
                            _logger.info(f"Imagen actualizada para el producto '{producto_local.name}'.")

            _logger.info("Migración de imágenes de productos completada.")

        except Exception as e:
            _logger.error(f"Error durante la migración de imágenes: {e}")


    def migrate_contacts(self):
        uid, models = self.connect_to_odoo()
        contacts = models.execute_kw(self.db, uid, self.password,
                                     'res.partner', 'search_read',
                                     [[]],
                                     {'fields': ['is_company', 'name', 'street', 'city', 'state_id', 'country_id',
                                                 'vat', 'function', 'phone', 'email', 'l10n_cl_dte_email']})
        
        for contact in contacts:
            try:
                # Preparación y mapeo de campos de Odoo 17 a Odoo 15
                local_contact_vals = {
                    'is_company': contact['is_company'],
                    'name': contact['name'],
                    'street': contact.get('street', ''),
                    # Asume que tienes campos personalizados en Odoo 15 para los siguientes datos
                    'document_number': contact.get('vat', ''),
                    'phone': contact.get('phone', ''),
                    'email': contact.get('email', ''),
                    'dte_email': contact.get('l10n_cl_dte_email', ''),
                    # Añade aquí cualquier otro mapeo de campos necesario
                }

                # Buscar si existe un contacto con el mismo nombre y número de documento en la base de datos local
                existing_contact = self.env['res.partner'].search([('name', '=', contact['name']), ('vat', '=', contact.get('vat', ''))], limit=1)
                
                if existing_contact:
                    existing_contact.write(local_contact_vals)
                    _logger.info(f"Contacto actualizado: {contact['name']}")
                else:
                    self.env['res.partner'].create(local_contact_vals)
                    _logger.info(f"Contacto creado: {contact['name']}")
                    
            except Exception as e:
                _logger.error(f"Error al migrar el contacto {contact['name']}: {e}")

        _logger.info("Migración de contactos completada.")


    def migrate_products(self):
        """
        Migra productos activos desde Odoo 12 (remoto) a Odoo 16 (local).
        Omite productos con códigos de barras ya migrados o existentes.
        """
        BATCH_SIZE = 100
        uid, models = self.connect_to_odoo()

        # Para evitar duplicados de barcode en memoria durante el proceso
        barcodes_seen = set()

        try:
            total_products = models.execute_kw(self.db, uid, self.password,
                                            'product.template', 'search_count', [[('active', '=', True)]])
            _logger.info('Total de productos activos a migrar: %s', total_products)

            for offset in range(0, total_products, BATCH_SIZE):
                product_batch = models.execute_kw(self.db, uid, self.password,
                                                'product.template', 'search_read',
                                                [[('active', '=', True)]],
                                                {'fields': ['name', 'default_code', 'list_price', 'standard_price', 'active', 'barcode'],
                                                'limit': BATCH_SIZE,
                                                'offset': offset})

                for product_data in product_batch:
                    try:
                        default_code = product_data.get('default_code')
                        name = product_data.get('name')
                        barcode = product_data.get('barcode')

                        # Verificar existencia por default_code o name
                        domain = [('default_code', '=', default_code)] if default_code else [('name', '=', name)]
                        existing_product = self.env['product.template'].sudo().search(domain, limit=1)
                        if existing_product:
                            _logger.info(f"Producto ya existe localmente: {existing_product.name}, se omite.")
                            continue

                        # Verificar si barcode ya existe en la BD local o ya se ha procesado en esta sesión
                        if barcode:
                            if barcode in barcodes_seen:
                                _logger.warning(f"Barcode duplicado ya visto en esta sesión: {barcode}, se omite.")
                                continue
                            existing_barcode = self.env['product.template'].sudo().search([('barcode', '=', barcode)], limit=1)
                            if existing_barcode:
                                _logger.warning(f"Barcode ya existe en local: {barcode}, se omite.")
                                continue
                            barcodes_seen.add(barcode)

                        product_vals = {
                            'name': name,
                            'default_code': default_code,
                            'list_price': product_data.get('list_price'),
                            'standard_price': product_data.get('standard_price'),
                            'active': product_data.get('active'),
                            'barcode': barcode,
                        }

                        new_product = self.env['product.template'].sudo().create(product_vals)
                        _logger.info(f"Producto creado: {new_product.name} con ID: {new_product.id}")

                    except Exception as e:
                        _logger.error(f"Error al procesar producto {product_data.get('name')}: {e}")

        except Exception as e:
            _logger.error(f"Error general en la migración de productos: {e}")
            raise UserError(f"Error general en la migración: {str(e)}")


    def download_product_batches(self):
        """
        Descarga productos activos desde la base remota y guarda cada lote como JSON.
        """
        BATCH_SIZE = 100
        uid, models = self.connect_to_odoo()

        try:
            total_products = models.execute_kw(self.db, uid, self.password,
                                            'product.template', 'search_count', [[('active', '=', True)]])
            _logger.info(f'Total de productos activos a migrar: {total_products}')

            for offset in range(0, total_products, BATCH_SIZE):
                product_batch = models.execute_kw(self.db, uid, self.password,
                                                'product.template', 'search_read',
                                                [[('active', '=', True)]],
                                                {'fields': ['name', 'default_code', 'list_price', 'standard_price', 'active', 'barcode'],
                                                'limit': BATCH_SIZE,
                                                'offset': offset})
                if product_batch:
                    self.env['product.data.json'].sudo().create({
                        'data': json.dumps(product_batch)
                    })
                    _logger.info(f'Lote de productos guardado en JSON. Offset: {offset}')

        except Exception as e:
            _logger.error(f'Error al descargar productos: {e}')



    def migrate_product_images(self):
        """
        Migra imágenes de productos de Odoo 17 a Odoo 15, incluyendo:

        - Manejo de errores potenciales durante la recuperación, conversión y actualización de imágenes.
        - Optimización del rendimiento con procesamiento por lotes y tiempos de espera.
        - Conversión de WebP a JPG o PNG.
        - Registro de mensajes informativos para depuración y seguimiento.

        **Parámetros:**
        - Ninguno

        **Devuelve:**
        - Ninguno
        """

        tamanio_lote = 100  # Ajusta este valor según sea necesario para tu caso de uso

        uid, models = self.connect_to_odoo()
        product_ids = models.execute_kw(self.db, uid, self.password, 'product.template', 'search', [[('external_id', '!=', False)]])
        _logger.info(f"Total de productos con imágenes para migrar: {len(product_ids)}")

        for i in range(0, len(product_ids), tamanio_lote):
            batch_ids = product_ids[i:i + tamanio_lote]
            products_data = models.execute_kw(self.db, uid, self.password, 'product.template', 'read', [batch_ids, ['external_id', 'image_1920']])

            for product_data in products_data:
                try:
                    producto_external_id = product_data.get('external_id')
                    if not producto_external_id:
                        _logger.warning(f"El producto con ID {product_data['id']} no tiene un ID externo.")
                        continue

                    producto_local = self.env['product.template'].search([('external_id', '=', producto_external_id)], limit=1)

                    if producto_local and not producto_local.image_1920:
                        try:
                            datos_imagen = product_data['image_1920']
                            if not datos_imagen:
                                _logger.warning(f"El producto '{producto_local.name}' no tiene datos de imagen.")
                                continue

                            # Conversión y optimización de la imagen
                            datos_imagen = self.convertir_y_optimizar_imagen(datos_imagen)

                            # Actualización del campo de imagen
                            producto_local.write({'image_1920': datos_imagen})
                            _logger.info(f"Imagen migrada para '{producto_local.name}'.")
                        except Exception as e:
                            _logger.error(f"Error al migrar la imagen para '{producto_local.name}': {e}")

                except Exception as e:
                    _logger.error(f"Error al obtener el ID externo del producto: {e}")

        _logger.info("Migración de imágenes de productos completada.")

    def convertir_y_optimizar_imagen(self, datos_imagen):
        """
        Convierte y optimiza la imagen de WebP a JPG.
        """
        try:
            if isinstance(datos_imagen, str):
                datos_imagen = base64.b64decode(datos_imagen)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".webp") as temp_file:
                temp_file.write(datos_imagen)
                temp_file.flush()

                with Image.open(temp_file.name) as img:
                    img_format = 'JPEG'
                    img.save(temp_file.name, format=img_format, optimize=True, quality=85)

                with open(temp_file.name, "rb") as f:
                    optimized_image = base64.b64encode(f.read()).decode('utf-8')

                os.remove(temp_file.name)
                return optimized_image
        except Exception as e:
            _logger.error(f"Error during image conversion and optimization: {e}")
            return base64.b64encode(datos_imagen).decode('utf-8')  # Return the original image in case of failure





    def download_product_batches(self):
        uid, models = self.connect_to_odoo()
        BATCH_SIZE = 500  # Definir el tamaño del lote

        try:
            total_products = models.execute_kw(self.db, uid, self.password, 'product.template', 'search_count', [[]])
            _logger.info(f'Total de productos a migrar: {total_products}')

            for offset in range(0, total_products, BATCH_SIZE):
                product_batch = models.execute_kw(self.db, uid, self.password, 'product.template', 'search_read', [[]],
                                                {'fields': ['name', 'default_code', 'list_price', 'standard_price', 'active', 'barcode'],
                                                'limit': BATCH_SIZE, 'offset': offset})
                
                if product_batch:
                    # Guardar este lote en el modelo product.data.json
                    self.env['product.data.json'].create({'data': json.dumps(product_batch)})
                    _logger.info(f'Lote de productos guardado. Offset: {offset}')

        except Exception as e:
            _logger.error(f'Error al descargar lotes de productos: {e}')


_logger = logging.getLogger(__name__)

class ProductDataJSON(models.Model):
    _name = 'product.data.json'
    _description = 'Datos de productos en formato JSON'
    
    data = fields.Text('Datos de producto', required=True)

    @api.model
    def process_stored_product_batches(self):
        batches = self.search([])
        for batch in batches:
            try:
                self.env.cr.commit()  # Fuerza guardar y reiniciar cursor entre lotes
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
                            p['barcode'] = False  # Elimina el duplicado

                        self.env['product.template'].sudo().create({
                            'name': name,
                            'default_code': p.get('default_code'),
                            'list_price': p.get('list_price', 0),
                            'standard_price': p.get('standard_price', 0),
                            'active': p.get('active', True),
                            'barcode': p.get('barcode'),
                        })

                        if (i + 1) % 10 == 0:
                            self.env.cr.commit()  # Comitea cada 10 para evitar cierre de cursor

                    except Exception as e:
                        _logger.error(f"Error con producto {p.get('name')}: {e}")
                        self.env.cr.rollback()

                batch.sudo().unlink()

            except Exception as e:
                _logger.error(f"Error al procesar lote: {e}")
                self.env.cr.rollback()

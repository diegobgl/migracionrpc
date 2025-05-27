# -*- coding: utf-8 -*-
import base64
import logging
from odoo import models, api, fields
import tempfile
import xmlrpc.client  # Importa el módulo xmlrpc.client
import json
import time



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


    def migrate_products(self):
        """
        Migra productos activos desde una base remota Odoo a la instancia local en lotes.
        Evita duplicados, ignora errores de códigos de barras duplicados y solo consulta datos remotos.
        """
        BATCH_SIZE = 100
        uid, models = self.connect_to_odoo()

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
                    default_code = product_data.get('default_code')
                    name = product_data.get('name')

                    # Evitar duplicados locales
                    domain = [('default_code', '=', default_code)] if default_code else [('name', '=', name)]
                    existing_product = self.env['product.template'].sudo().search(domain, limit=1)

                    if existing_product:
                        _logger.info(f"Producto ya existe localmente: {existing_product.name}, se omite.")
                        continue

                    product_vals = {
                        'name': name,
                        'default_code': default_code,
                        'list_price': product_data.get('list_price'),
                        'standard_price': product_data.get('standard_price'),
                        'active': product_data.get('active'),
                        'barcode': product_data.get('barcode'),
                    }

                    try:
                        new_product = self.env['product.template'].sudo().create(product_vals)
                        _logger.info(f"Producto creado: {new_product.name} con ID: {new_product.id}")
                    except Exception as e:
                        # Intenta nuevamente sin barcode si falla
                        _logger.warning(f"Error al crear producto '{name}' con barcode '{product_vals.get('barcode')}': {e}")
                        product_vals['barcode'] = False
                        try:
                            new_product = self.env['product.template'].sudo().create(product_vals)
                            _logger.info(f"Producto creado sin barcode: {new_product.name}")
                        except Exception as e2:
                            _logger.error(f"No se pudo crear el producto '{name}' ni siquiera sin barcode: {e2}")

        except Exception as e:
            _logger.error(f"Error general en la migración de productos: {e}")
            raise UserError(f"Error general en la migración: {str(e)}")





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

    def process_stored_product_batches(self):
        product_batches = self.env['product.data.json'].search([])

        for batch in product_batches:
            products = json.loads(batch.data)
            for product_data in products:
                # Preparar valores para la creación del producto (omitimos la categoría según indicaciones)
                product_vals = {
                    'name': product_data['name'],
                    'default_code': product_data['default_code'],
                    'list_price': product_data['list_price'],
                    'standard_price': product_data['standard_price'],
                    'active': product_data['active'],
                    'barcode': product_data['barcode'],
                    # 'categ_id': No se asigna categoría
                    # Asegúrate de agregar cualquier otro campo necesario específico para 'product.product'
                }
                # Crear el producto en la base de datos local usando 'product.product', como superusuario
                new_product = self.env['product.product'].sudo().create(product_vals)
                _logger.info(f"Producto creado como superusuario: {new_product.name} con ID: {new_product.id}")

            # Opcional: Eliminar el lote procesado de product.data.json para limpiar, como superusuario
            batch.sudo().unlink()
            _logger.info('Lote de productos procesado y eliminado como superusuario.')

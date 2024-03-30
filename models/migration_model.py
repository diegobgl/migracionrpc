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
                    time.sleep(wait_seconds)  # Aquí es donde se usa time.sleep()
                    wait_seconds *= 2  # Incrementa el tiempo de espera para el próximo intento
                else:
                    raise e
        raise Exception("Max retries reached for execute_kw_with_retry")
        

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
        Función para migrar productos desde una base de datos Odoo remota a la local.
        """
        BATCH_SIZE = 100
        for record in self:
            try:
                # Conectar a la base de datos remota
                uid, models = record.connect_to_odoo()
                if not uid:
                    raise ConnectionError("No se pudo establecer la conexión XML-RPC con la base de datos remota.")

                # Obtener el número total de productos en la base de datos remota
                product_count = models.execute_kw(record.db, uid, record.password, 'product.template', 'search_count', [[]])

                # Almacenamiento de datos de productos en una lista
                products_data = []
                offset = 0

                while offset < product_count:
                    # Consultar IDs de productos en lotes
                    product_ids = models.execute_kw(
                        record.db, uid, record.password,
                        'product.template', 'search',
                        [[]],
                        {'offset': offset, 'limit': BATCH_SIZE}
                    )

                    # Obtener datos de productos en bloque
                    lote_productos = models.execute_kw(
                        record.db, uid, record.password,
                        'product.template', 'read',
                        [product_ids, ['name', 'list_price', 'standard_price', 'description_sale', 'default_code']]
                    )

                    products_data.extend(lote_productos)  # Añadir datos de forma eficiente
                    offset += BATCH_SIZE

                # Crear registros de productos en la base de datos local
                registros_productos = self.env['product.template'].create(products_data)

                _logger.info(f"Se migraron {len(registros_productos)} productos correctamente desde '{record.url}'.")
            except (ConnectionError, xmlrpc.client.Fault) as e:
                _logger.error(f"Error durante la migración de productos desde '{record.url}': {e}")

    def migrate_product_images(self):
        uid, models = self.connect_to_odoo()
        BATCH_SIZE = 100  # Ajusta esto según la capacidad de tu servidor
        wait_seconds = 5  # Tiempo de pausa para evitar sobrecargar el servidor

        # Busca productos en Odoo 12 que tienen 'image_medium' definido
        product_ids = models.execute_kw(self.db, uid, self.password, 'product.template', 'search', [[('image_medium', '!=', False)]])
        _logger.info(f"Total de productos con imágenes a migrar: {len(product_ids)}")

        for i in range(0, len(product_ids), BATCH_SIZE):
            batch_ids = product_ids[i:i + BATCH_SIZE]
            # Recupera el 'image_medium' de los productos en Odoo 12
            products_data = models.execute_kw(self.db, uid, self.password, 'product.template', 'read', [batch_ids, ['name', 'image_medium']])
            
            for product_data in products_data:
                local_product = self.env['product.template'].search([('name', '=', product_data['name'])], limit=1)
                # Escribe 'image_1920' en el producto correspondiente en Odoo 16
                if local_product and not local_product.image_1920:
                    # Asegúrate de convertir la imagen a base64 si es necesario
                    local_product.write({'image_1920': product_data['image_medium']})
                    _logger.info(f"Imagen migrada para {local_product.name}")
            
            time.sleep(wait_seconds)  # Pausa entre lotes para mitigar el riesgo de sobrecargar el servidor
            
        _logger.info("Migración de imágenes completada.")


_logger = logging.getLogger(__name__)

class ProductDataJSON(models.Model):
    _name = 'product.data.json'
    _description = 'Datos de productos en formato JSON'
    

    data = fields.Text('Datos de producto', required=True)

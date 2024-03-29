# -*- coding: utf-8 -*-
from odoo import models, fields, api
import xmlrpc.client
import logging
_logger = logging.getLogger(__name__)


class ProductMigration(models.Model):
    _name = 'product.migration'
    _description = 'Migración de Productos'

    # Campos para la conexión
    url = fields.Char('URL de Odoo 16')
    db = fields.Char('Base de Datos')
    username = fields.Char('Usuario')
    password = fields.Char('Contraseña')

    # # Método para conectarse a Odoo 16
    # def connect_to_odoo16(self):
    #     common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(self.url))
    #     uid = common.authenticate(self.db, self.username, self.password, {})
    #     models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self.url))
    #     return uid, models


    def connect_to_odoo16(self):
        try:
            common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(self.url))
            uid = common.authenticate(self.db, self.username, self.password, {})
            models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self.url))
            return uid, models
        except Exception as e:
            _logger.error('Error al conectar a Odoo 16: %s', e)
            raise

    # Método para obtener los productos de Odoo 16
    @api.model
    def get_products_from_odoo16(self, cr):
        uid, models = self.connect_to_odoo16()
        product_ids = models.execute_kw(self.db, uid, self.password,
            'product.product', 'search', [[]],
            {'limit': 10})  # Ejemplo: limitar a 10 productos
        products = models.execute_kw(self.db, uid, self.password,
            'product.product', 'read', [product_ids])
        return products

    # Método para procesar y guardar los productos en Odoo 17
    @api.model
    def process_and_save_products(self, products):
        for product in products:
            # Aquí se procesan los datos y se adaptan a Odoo 17
            # ...
            # Guardar el producto en Odoo 17
            self.env['product.product'].create({
                'name': product['name'],
                # otros campos...
            })

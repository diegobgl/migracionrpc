# -*- coding: utf-8 -*-
{
    'name': "migracionrpc",

    'summary': "Módulo para la migración de datos entre instancias de Odoo",

    'description': """
        Este módulo permite la migración de datos de productos entre
        Odoo 16 y Odoo 17 utilizando XML-RPC para la comunicación entre instancias.
    """,

    'author': "DG software spa",
    'website': "https://www.dgdev.cl",

    'category': 'Custom',
    'version': '0.1',

    'depends': ['base', 'product'],

    'data': [
         'security/ir.model.access.csv',
        'views/migration_views.xml',
        'views/lot_view.xml',
        'views/migration_menu.xml'
        'views/server_action.xml',
    ],
    'demo': [
        'demo/demo.xml',
    ],
}

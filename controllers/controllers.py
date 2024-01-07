# -*- coding: utf-8 -*-
# from odoo import http


# class Migracionrpc(http.Controller):
#     @http.route('/migracionrpc/migracionrpc', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/migracionrpc/migracionrpc/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('migracionrpc.listing', {
#             'root': '/migracionrpc/migracionrpc',
#             'objects': http.request.env['migracionrpc.migracionrpc'].search([]),
#         })

#     @http.route('/migracionrpc/migracionrpc/objects/<model("migracionrpc.migracionrpc"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('migracionrpc.object', {
#             'object': obj
#         })


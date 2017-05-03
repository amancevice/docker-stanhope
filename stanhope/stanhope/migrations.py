""" Stanhope Framers Migrations. """
import ardec
import pandas
from .tables import *


class StanhopeFramers(ardec.migration):
    def __init__(self, opened, closed, archived):
        super(StanhopeFramers, self).__init__()
        tables = [x for x in [opened, closed, archived] if x]
        self.customers = Customers()
        self.frameorders = FrameOrders(*tables)
        self.accounts = None
        self.contacts = None
        self.orders = None
        self.treatments = None

    @ardec.stage('load_customers')
    def load_customers(self):
        self.customers.load()

    @ardec.stage('load_orders')
    def load_frameorders(self):
        self.frameorders.load()

    @ardec.stage('join_records')
    def join_records(self):
        customers = self.customers.frame['Customer Number']\
                        .isin(self.frameorders.frame['CustomerNo'])
        self.customers.frame = self.customers.frame.loc[customers]

    @ardec.stage('export_accounts')
    def export_accounts(self):
        self.accounts = self.customers.accounts()

    @ardec.stage('export_contacts')
    def export_contacts(self):
        self.contacts = self.customers.contacts()

    @ardec.stage('export_orders')
    def export_orders(self):
        self.orders = self.frameorders.orders()

    @ardec.stage('export_treatments')
    def export_treatments(self):
        self.treatments = self.frameorders.treatments()

    @ardec.stage('write_csv')
    def write_csv(self):
        kwargs = {'index': False, 'date_format': '%m/%d/%Y %H:%M:%S'}
        path = "/data/{}.csv"
        self.accounts.to_csv(path.format('Accounts'), **kwargs)
        self.contacts.to_csv(path.format('Contacts'), **kwargs)
        self.orders.to_csv(path.format('Orders'), **kwargs)
        self.treatments.to_csv(path.format('Treatments'), **kwargs)

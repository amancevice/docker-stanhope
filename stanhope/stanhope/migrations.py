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
        return self.customers.load()

    @ardec.stage('load_orders')
    def load_frameorders(self):
        return self.frameorders.load()

    @ardec.stage('join_records')
    def join_records(self):
        frame_cust = self.frameorders.frame['CustomerNo']
        all_cust = self.customers.frame['Customer Number']
        cust = set(frame_cust) & set(all_cust)
        customers = self.customers.frame['Customer Number'].isin(cust)
        self.customers.frame = \
            self.customers.frame.loc[
                self.customers.frame['Customer Number'].isin(cust)]\
            .reset_index(drop=True)
        self.frameorders.frame = \
            self.frameorders.frame.loc[
                self.frameorders.frame['CustomerNo'].isin(cust)]\
            .reset_index(drop=True)
        return self.customers.frame

    @ardec.stage('export_accounts')
    def export_accounts(self):
        self.accounts = self.customers.accounts()
        return self.accounts

    @ardec.stage('export_contacts')
    def export_contacts(self):
        self.contacts = self.customers.contacts()
        return self.contacts

    @ardec.stage('export_orders')
    def export_orders(self):
        self.orders = self.frameorders.orders()
        return self.orders

    @ardec.stage('export_treatments')
    def export_treatments(self):
        self.treatments = self.frameorders.treatments()
        return self.treatments

    @ardec.stage('write_csv')
    def write_csv(self):
        kwargs = {'index': False, 'date_format': '%m/%d/%Y %H:%M:%S'}
        path = "/data/{}.csv"
        self.accounts.to_csv(path.format('Accounts'), **kwargs)
        self.contacts.to_csv(path.format('Contacts'), **kwargs)
        self.orders.to_csv(path.format('Orders'), **kwargs)
        self.treatments.to_csv(path.format('Treatments'), **kwargs)

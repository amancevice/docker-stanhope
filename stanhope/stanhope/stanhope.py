'''
Stanhope Framers Data Migration

Usage:
    stanhope.py [options]

Options:
    -i --interactive  # Open IPython session before writing
    -a --all          # Export all customers
'''

import fractions
import re

import ardec
import pandas
import IPython
from docopt import docopt


@ardec.stage('write_csv')
def write_csv(frame, path):
    frame.to_csv(path, index=False, date_format='%m/%d/%Y %H:%M:%S')


def try_or_nan(func):
    def trywrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            return pandas.np.nan
    return trywrap


@try_or_nan
def upper(value):
    return value.upper()


@try_or_nan
def mapping(value, **mapping):
    return mapping[value]


@try_or_nan
def knackstamp(value):
    return pandas.Timestamp(value).strftime('%m/%d/%Y %H:%M:%S')


def dimen(value):
    try:
        return fractions.Fraction(value)
    except ValueError:
        if '/' in value and (' ' in value or '.' in value):
            wholestr, fracstr = re.split(r'[. ]', value)
            whole = int(wholestr)
            return int(whole) + fractions.Fraction(fracstr)
        else:
            return pandas.np.nan


@try_or_nan
def inches(value):
    return int(dimen(value))


def fraction(value):
    try:
        dimen = dimen(value)
        frac = str(dimen - int(dimen))
        return ' {}\''.format(frac) if frac != '0' else '"'
    except:
        return '"'


class Migration(ardec.migration):
    def __init__(self, *paths):
        super(Migration, self).__init__(self.__name__)
        self.paths = paths

    def _read_csv(self, *args, **kwargs):
        return pandas.concat([
            pandas.read_csv(x, *args, **kwargs) for x in self.paths])


class OrdersMigration(Migration):
    __name__ = 'Orders'

    @staticmethod
    def status(value):
        return mapping(value, **{
            'O': 'Open',
            'C': 'Closed',
            'A': 'Archived',
            'X': 'Closed'})

    @staticmethod
    def order_location(value):
        return mapping(value, BOS='Boston', SOM='Somerville')

    @staticmethod
    def delivery_location(value):
        return mapping(value, **{'UPS': 'Delivery', 'PU BOS': 'Boston'})

    @staticmethod
    def sales_type(value):
        return mapping(value,
                       CONS='Conservation', HOLD='Hold', MATS='Mats',
                       MET='Metal', OTH='Other', SPO='Special Order',
                       VOID='Void', WHF='Hand-Finished', WPF='Pre-Finished')

    @staticmethod
    def discount(value):
        return mapping(value, **{
            'Artist': 'Artist',
            'Dealer': 'Dealer',
            'Donation': 'Donation',
            'Employee': 'Employee',
            'Institution': 'Institution',
            'Other': 'Other',
            'Poster Special': 'Poster',
            'SMFA Art Sale': 'SMFA',
            'Smfa Art Sale': 'SMFA',
            'Verbal': 'Verbal',
            'WBUR': 'WBUR',
            'Wbur': 'WBUR',
            'gallery': 'Gallery',
            'special': 'Special'})

    @staticmethod
    def join(value):
        return mapping(value, Nailed='Nailed', Splined='Splined')

    @staticmethod
    def matmfg(value):
        return mapping(value, **{
            'ALPHA/RISING': pandas.np.nan,
            'Alpha': 'Alpha',
            'FC': pandas.np.nan,
            'Linen': pandas.np.nan,
            'Linen/rag': pandas.np.nan,
            'Linen/rising': pandas.np.nan,
            'MASONITE': pandas.np.nan,
            'Other': pandas.np.nan,
            'Pongee Silk': 'Pongee',
            'Provided': pandas.np.nan,
            'RISING/ALPHA': pandas.np.nan,
            'Rising': 'Rising',
            'SPO': pandas.np.nan,
            'Shantung Silk': 'Shantung',
            'customer mats': pandas.np.nan,
            'fome core': pandas.np.nan,
            'provided': pandas.np.nan,
            'rising/alpha': pandas.np.nan})

    @property
    @ardec.stage('legacy')
    def legacy(self):
        converters = {
            'OrderDate': knackstamp,
            'DueDate': knackstamp,
            'DateCompleted': knackstamp}
        return self._read_csv(converters=converters)

    @property
    @ardec.stage('orders')
    def orders(self):
        converters = {
            'Delivery Location': self.delivery_location,
            'Discount': self.discount,
            'Due Date': knackstamp,
            'Legacy Customer Number': upper,
            'Order Date': knackstamp,
            'Order Location': self.order_location,
            'Order Number': upper,
            'Order Status': self.status}
        names = [
            'Order Number',
            'Order Date',
            'Due Date',
            'SalesCatgy',
            'Legacy Customer Number',
            'Qty',
            'TotalSale',
            'Order Status',
            'Order Location',
            'Salesperson',
            'Delivery Location',
            'Artist',
            'Comments',
            'FrameMfg',
            'SalesType',
            'DateCompleted',
            'FrameNo',
            'Discount',
            'Cust-Client',
            'Joining',
            'Frame Width',
            'Frame Height',
            'Mat',
            'MatMfg',
            'MatColor',
            'MattingSize',
            'Glazing',
            'ProductionComments',
            'BinNo',
            'Matting',
            'Fitting']
        columns = [
            'Order Number',
            'Account Link',
            'Salesperson',
            'Order Date',
            'Due Date',
            'Order Location',
            'Order Status',
            'Delivery Location',
            'Legacy Order Number Link',
            'Legacy Customer Number']
        frame = self._read_csv(converters=converters, names=names, skiprows=1)
        frame.loc[:, 'Account Link'] = frame['Legacy Customer Number']
        frame.loc[:, 'Legacy Order Number Link'] = frame['Order Number']
        return frame[columns]

    @property
    @ardec.stage('treatments')
    def treatments(self):
        converters = {
            'Frame Join': self.join,
            'Mat Manufacturer': self.matmfg,
            'Order Number': upper,
            'Type': self.sales_type,
            'Order Status': self.status}
        names = [
            'Order Number',
            'OrderDate',
            'DueDate',
            'SalesCatgy',
            'CustomerNo',
            'Quantity',
            'Price',
            'Order Status',
            'Location',
            'SalesPers',
            'Delivery',
            'Artist',
            'Description',
            'Frame Manufacturer',
            'Type',
            'DateCompleted',
            'Frame Style',
            'Discount',
            'Cust-Client',
            'Frame Join',
            'Frame Width',
            'Frame Height',
            'Mat',
            'Mat Manufacturer',
            'Mat Color',
            'Mat Size',
            'Glazing',
            'Production Comments',
            'BinNo',
            'Matting',
            'Fitting']
        columns = [
            'Order Number',
            'Order Status',
            'Type',
            'Quantity',
            'Frame Width Inches',
            'Frame Width Fraction',
            'Frame Height Inches',
            'Frame Height Fraction',
            # 'Frame Manufacturer',
            'Frame Style',
            'Frame Join',
            # 'Frame Strainer',
            # 'Frame Recess',
            # 'Frame Buildup',
            # 'Matting / Mounting',
            'Mat Manufacturer',
            # 'Mat Hinge',
            'Mat Size',
            'Mat Color',
            # 'Glazing',
            'Price',
            'Artist',
            'Description',
            'Production Comments']
        frame = self._read_csv(converters=converters, names=names, skiprows=1)
        frame.loc[:, 'Frame Width Inches'] = \
            frame['Frame Width'].apply(inches)
        frame.loc[:, 'Frame Width Fraction'] = \
            frame['Frame Width'].apply(fraction)
        frame.loc[:, 'Frame Height Inches'] = \
            frame['Frame Height'].apply(inches)
        frame.loc[:, 'Frame Height Fraction'] = \
            frame['Frame Height'].apply(fraction)
        return frame[columns]


class CustomersMigration(Migration):
    __name__ = 'Customers'

    def _account_type(self, frame):
        idx = frame['Tax Exempt'] | frame['Credit']
        frame.loc[idx, 'Account Type'] = 'Institution'
        frame.loc[~idx, 'Account Type'] = 'Personal'
        return frame

    @property
    @ardec.stage('legacy')
    def legacy(self):
        converters = {
            'Last Update': knackstamp,
            'Last Order': knackstamp,
            'Date': knackstamp,
            'Customer Number': upper}
        return self._read_csv(converters=converters)

    @property
    @ardec.stage('accounts')
    def accounts(self):
        converters = {'Legacy Customer Number': upper}
        names = ['Legacy Customer Number', 'Account', 'Category', 'Address',
                 'City', 'State', 'Zip', 'Telephone', 'Credit', 'Comment',
                 'Tax Exempt', 'Source', 'Date', 'Last Update', 'Last Order',
                 'Email', 'Deceased']
        columns = ['Account Type', 'Account', 'Tax Exempt', 'Credit',
                   'Legacy Customer Number', 'Legacy Custom Number Link']
        frame = self._read_csv(converters=converters, names=names, skiprows=1)
        frame.loc[:, 'Account Type'] = self._account_type(frame)
        frame.loc[:, 'Legacy Custom Number Link'] = \
            frame['Legacy Customer Number']
        return frame[columns]

    @property
    @ardec.stage('contacts')
    def contacts(self):
        converters = {'Legacy Customer Number': upper}
        names = ['Legacy Customer Number', 'Contact', 'Category', 'Address',
                 'City', 'State', 'Zip', 'Telephone', 'Credit', 'Comment',
                 'Tax Exempt', 'Source', 'Date', 'Last Update', 'Last Order',
                 'Email', 'Deceased']
        columns = ['Legacy Customer Number', 'Contact', 'Email', 'Address',
                   'City', 'State', 'Zip', 'Telephone', 'Deceased']
        frame = self._read_csv(converters=converters, names=names, skiprows=1)
        return frame[columns]


def main(args):
    with OrdersMigration('/data/MSAccess/FrameOrders-Working.csv',
                         '/data/MSAccess/FrameOrders-Closed.csv') as migration:
        legacy_orders = migration.legacy
        orders = migration.orders
        treatments = migration.treatments

    with CustomersMigration('/data/MSAccess/Customers.csv') as migration:
        legacy_customers = migration.legacy
        accounts = migration.accounts
        contacts = migration.contacts

    if args['--interactive']:
        IPython.embed()
    else:
        if not args['--all']:
            cust_num = orders['Legacy Customer Number']
            lgcy_idx = legacy_customers['Customer Number'].isin(cust_num)
            acct_idx = accounts['Legacy Customer Number'].isin(cust_num)
            cust_idx = contacts['Legacy Customer Number'].isin(cust_num)
            legacy_customers = legacy_customers.loc[lgcy_idx]
            accounts = accounts.loc[acct_idx]
            contacts = contacts.loc[acct_idx]

        with ardec.migration('Write CSVs') as migration:
            write_csv(legacy_customers, '/data/KnackHQ/LegacyCustomers.csv')
            write_csv(accounts, '/data/KnackHQ/Accounts.csv')
            write_csv(contacts, '/data/KnackHQ/Contacts.csv')
            write_csv(legacy_orders, '/data/KnackHQ/LegacyOrders.csv')
            write_csv(orders, '/data/KnackHQ/Orders.csv')
            write_csv(treatments, '/data/KnackHQ/Treatments.csv')


if __name__ == '__main__':
    main(docopt(__doc__))

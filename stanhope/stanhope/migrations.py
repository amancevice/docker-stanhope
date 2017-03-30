""" Stanhope Framers Migrations. """
import ardec
import pandas
from stanhope import utils


class Migration(ardec.migration):
    TABLES = []
    CONVERTERS = {}

    def _read_csv_stage(self, table, *args, **kwargs):
        with ardec.stage("export_{table}".format(table=table)):
            return utils.export(table, *args, **kwargs)

    def read_csv(self, *args, **kwargs):
        kwargs.setdefault('converters', self.CONVERTERS)
        return pandas.concat([self._read_csv_stage(x, *args, **kwargs)
                              for x in self.TABLES])

    @staticmethod
    @ardec.stage('write_csv')
    def write_csv(frame, path):
        frame.to_csv(path, index=False, date_format='%m/%d/%Y %H:%M:%S')

    def migrate(self):
        path = "/data/{name}.csv".format(name=type(self).__name__)
        frame = self.read_csv()
        self.write_csv(frame, path)
        return frame


class LegacyCustomers(Migration):
    TABLES = ['Customers']
    CONVERTERS = {
        'Customer Number': utils.upper,
        'Date': utils.knackstamp,
        'Last Order': utils.knackstamp,
        'Last Update': utils.knackstamp}


class Accounts(LegacyCustomers):
    CONVERTERS = {
        'Legacy Customer Number': utils.upper}
    HEADER = [
        'Legacy Customer Number',
        'Account',
        'Category',
        'Address',
        'City',
        'State',
        'Zip',
        'Telephone',
        'Credit',
        'Comment',
        'Tax Exempt',
        'Source',
        'Date',
        'Last Update',
        'Last Order',
        'Email',
        'Deceased']
    COLUMNS = [
        'Account Type',
        'Account',
        'Tax Exempt',
        'Credit',
        'Legacy Customer Number',
        'Legacy Customer Number Link']

    def read_csv(self, *args, **kwargs):
        frame = super(Accounts, self).read_csv(converters=self.CONVERTERS,
                                               names=self.HEADER,
                                               skiprows=1)
        frame['Account Type'] = utils.account_type(frame)
        frame['Legacy Customer Number Link'] = frame['Legacy Customer Number']
        return frame[self.COLUMNS]


class Contacts(LegacyCustomers):
    CONVERTERS = {
        'Legacy Customer Number': utils.upper}
    HEADER = [
        'Legacy Customer Number',
        'Contact',
        'Category',
        'Address',
        'City',
        'State',
        'Zip',
        'Telephone',
        'Credit',
        'Comment',
        'Tax Exempt',
        'Source',
        'Date',
        'Last Update',
        'Last Order',
        'Email',
        'Deceased']
    COLUMNS = [
        'Contact',
        'Email',
        'Address',
        'City',
        'State',
        'Zip',
        'Telephone',
        'Deceased',
        'Account Link']

    def read_csv(self, *args, **kwargs):
        frame = super(Contacts, self).read_csv(converters=self.CONVERTERS,
                                               names=self.HEADER,
                                               skiprows=1)
        frame['Account Link'] = frame['Legacy Customer Number']
        return frame[self.COLUMNS]


class LegacyOrders(Migration):
    TABLES = [
        'FrameOrders-Working',
        'FrameOrders-Closed',
        'FrameOrders-Archive']
    CONVERTERS = {
        'DateCompleted': utils.knackstamp,
        'DueDate': utils.knackstamp,
        'OrderDate': utils.knackstamp}


class Orders(LegacyOrders):
    CONVERTERS = {
        'Delivery Location': utils.delivery_location,
        'Discount': utils.discount,
        'Due Date': utils.knackstamp,
        'Legacy Customer Number': utils.upper,
        'Order Date': utils.knackstamp,
        'Order Location': utils.order_location,
        'Order Number': utils.upper,
        'Order Status': utils.status}
    HEADER = [
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
    COLUMNS = [
        'Order Number',
        'Order Date',
        'Due Date',
        'Order Location',
        'Order Status',
        'Delivery Location',
        'Account Link',
        'Salesperson Link',
        'Legacy Order Number Link']

    def read_csv(self, *args, **kwargs):
        frame = super(Orders, self).read_csv(converters=self.CONVERTERS,
                                             names=self.HEADER,
                                             skiprows=1)
        frame['Account Link'] = frame['Legacy Customer Number']
        frame['Salesperson Link'] = frame['Salesperson']
        frame['Legacy Order Number Link'] = frame['Order Number']
        return frame[self.COLUMNS]


class Treatments(LegacyOrders):
    CONVERTERS = {
        'Frame Join': utils.join,
        'Mat Manufacturer': utils.matmfg,
        'Order Number': utils.upper,
        'Type': utils.sales_type,
        'Order Status': utils.status}
    HEADER = [
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
    COLUMNS = [
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
        'Production Comments',
        'Order Link']

    def read_csv(self, *args, **kwargs):
        frame = super(Treatments, self).read_csv(converters=self.CONVERTERS,
                                                 names=self.HEADER,
                                                 skiprows=1)
        frame['Order Link'] = frame['Order Number']
        frame['Frame Width Inches'] = \
            frame['Frame Width'].apply(utils.inches)
        frame['Frame Width Fraction'] = \
            frame['Frame Width'].apply(utils.fraction)
        frame['Frame Height Inches'] = \
            frame['Frame Height'].apply(utils.inches)
        frame['Frame Height Fraction'] = \
            frame['Frame Height'].apply(utils.fraction)
        return frame[self.COLUMNS]

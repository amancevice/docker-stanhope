""" Stanhope Framers Utils. """
import fractions
import io
import re
import subprocess

import pandas


def legacy_customer_record(row):
    row = row.replace(False, pandas.np.nan).dropna()
    if 'Comment' in row and '\n' in row['Comment']:
        comment = row['Comment']
        row.drop('Comment', inplace=True)
        return "<pre>\n{record}\nComment\n{comment}\n</pre>"\
               .format(record=row.to_string(), comment=comment)
    return "<pre>\n{record}\n</pre>".format(record=row.to_string())


def legacy_order_record(row):
    row = row.dropna()
    return "<pre>\n{record}\n</pre>".format(record=row.to_string())


def export(table, *args, **kwargs):
    """ Export table from StanhopeFramers.mdb.

        Arguments:
            table (str):  Name of table to export

        Returns:
            DataFrame of table.
    """
    cmd = ['mdb-export', '/data/StanhopeFramers.mdb', table]
    out = subprocess.check_output(cmd)
    return pandas.read_csv(io.BytesIO(out), *args, **kwargs)


def account_category(value):
    return mapping(value.strip(), **{
        'Artist': 'Artist',
        'Dealer': 'Dealer',
        'Employee': 'Employee',
        'Gallery': 'Gallery',
        'Other': 'Other',
        'Retail': 'Retail'})


def try_or_nan(func):
    def trywrap(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            return pandas.np.nan
    return trywrap


@try_or_nan
def knackstamp(value):
    return pandas.Timestamp(value).strftime('%m/%d/%Y %H:%M:%S')


@try_or_nan
def upper(value):
    return value.upper().strip()


@try_or_nan
def replace_newline(value, replace=r' '):
    return re.subn('[\n\r]+', replace, value)[0]\
             .replace(u'\x0b', replace)\
             .replace(u'\x10', replace)\
             .strip()\
             .strip('`')


@try_or_nan
def mapping(value, **mapping):
    return mapping[value]


def source(value):
    return mapping(value.strip().strip('.').upper(), **{
        'ART NE': 'Art New England',
        'BAY WIN': 'BAY WIN',
        'CN': 'CN',
        'CO REF': 'CO REF',
        'COUPON': 'Coupon',
        'CR': 'CR',
        'PC': 'PC',
        'SMFA SALE': 'SMFA Sale',
        'WBUR': 'WBUR',
        'WEB': 'Web',
        'WI': 'Walk In',
        'WLK IN': 'Walk In',
        'WLK': 'Walk In',
        'YLW BK': 'Yellow Pages',
        'YLW PG': 'Yellow Pages',
        'YP': 'Yellow Pages'})


def delivery_location(value):
    return mapping(value, **{'UPS': 'Delivery', 'PU BOS': 'Boston'})


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


def order_location(value):
    return mapping(value, BOS='Boston', SOM='Somerville')


def salesperson(value):
    if pandas.isnull(value) or not value:
        return 'RS'
    elif value == 'SN':
        return 'SB'
    return value


def status(value):
    return mapping(value, **{
        'O': 'Open',
        'C': 'Closed',
        'A': 'Closed',
        'X': 'Closed'})


def join(value):
    return mapping(value, Nailed='Nailed', Splined='Splined')


def matmfg(value):
    return mapping(value, **{
        'Alpha': 'Alpha',
        'Pongee Silk': 'Pongee',
        'Rising': 'Rising',
        'Shantung Silk': 'Shantung'})


def sales_type(value):
    return mapping(value,
                   CONS='Conservation', HOLD='Hold', MATS='Mats',
                   MET='Metal', OTH='Other', SPO='Special Order',
                   VOID='Void', WHF='Hand-Finished', WPF='Pre-Finished')


def dimen(value):
    try:
        return fractions.Fraction(value)
    except ValueError:
        if not pandas.isnull(value) \
                and '/' in value \
                and (' ' in value or '.' in value):
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
        dim = dimen(value)
        if dim.denominator not in [2, 4, 8, 16]:
            raise ValueError
        frac = str(dim - int(dim))
        return '.{}'.format(frac) if frac != '0' else pandas.np.nan
    except Exception as err:
        return pandas.np.nan


def boolean(value):
    return value == '1'


def legacy_order_id(row):
    return "{order}-{id}".format(order=row['OrderNo'], id=row.name)

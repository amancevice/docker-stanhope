""" Stanhope Framers Utils. """
import fractions
import io
import subprocess

import pandas


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


def account_type(frame):
    idx = frame['Tax Exempt'] | frame['Credit']
    return idx.apply(lambda x: 'Institution' if x else 'Personal')


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
    return value.upper()


@try_or_nan
def mapping(value, **mapping):
    return mapping[value]


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
        frac = str(dim - int(dim))
        return '.{}\"'.format(frac) if frac != '0' else '"'
    except Exception as err:
        return '"'

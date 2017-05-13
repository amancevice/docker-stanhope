""" Stanhope Framers Data Migration """
import IPython
import click
from . import options
from .migrations import *


@click.command()
@options.ARCHIVED
@options.CLOSED
@options.EPOCH
@options.INTERACTIVE
@options.OPENED
def stanhope(archived, closed, epoch, interactive, opened):
    """ Stanhope Framers Data Migration """
    with StanhopeFramers(opened, closed, archived, epoch) as mdb:
        customers = mdb.load_customers()
        frameorders = mdb.load_frameorders()
        mdb.time_filter()
        mdb.join_records()
        accounts = mdb.export_accounts()
        contacts = mdb.export_contacts()
        orders = mdb.export_orders()
        treatments = mdb.export_treatments()
        mdb.report()
        mdb.write_csv()

    if interactive is True:
        IPython.embed()

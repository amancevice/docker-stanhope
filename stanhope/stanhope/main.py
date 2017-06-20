""" Stanhope Framers Data Migration """
import IPython
import click
from . import options
from .migrations import StanhopeFramers


@click.command()
@options.ARCHIVED
@options.CLOSED
@options.EPOCH
@options.INTERACTIVE
@options.JOIN
@options.OPENED
def stanhope(archived, closed, epoch, interactive, join, opened):
    """ Stanhope Framers Data Migration """
    with StanhopeFramers(opened, closed, archived) as mdb:
        customers = mdb.load_customers()
        frameorders = mdb.load_frameorders()
        mdb.time_filter(epoch)
        mdb.join_records(join)
        accounts = mdb.export_accounts()
        contacts = mdb.export_contacts()
        orders = mdb.export_orders()
        treatments = mdb.export_treatments()
        mdb.report()
        mdb.write_csv()

    if interactive is True:
        IPython.embed()

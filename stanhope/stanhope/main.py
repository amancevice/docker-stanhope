""" Stanhope Framers Data Migration """
import IPython
import click
from . import options
from .migrations import *


@click.command()
@options.ARCHIVED
@options.CLOSED
@options.INTERACTIVE
@options.OPENED
def stanhope(archived, closed, interactive, opened):
    """ Stanhope Framers Data Migration """
    with StanhopeFramers(opened, closed, archived) as mdb:
        mdb.load_customers()
        mdb.load_frameorders()
        mdb.join_records()
        mdb.export_accounts()
        mdb.export_contacts()
        mdb.export_orders()
        mdb.export_treatments()
        mdb.write_csv()

    # Interact
    if interactive is True:
        IPython.embed()

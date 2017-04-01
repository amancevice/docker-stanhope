""" Stanhope Framers Data Migration """
import IPython
import click
from stanhope.migrations import *


@click.command()
@click.option('-a', '--archived', is_flag=True)
@click.option('-c', '--closed', is_flag=True)
@click.option('-i', '--interactive', is_flag=True)
@click.option('-o', '--opened', is_flag=True)
def stanhope(archived, closed, interactive, opened):
    with LegacyCustomers() as migration:
        legacy_customers = migration.migrate()
    with LegacyOrders(archived, closed, opened) as migration:
        legacy_orders = migration.migrate()
    with Accounts() as migration:
        accounts = migration.migrate()
    with Contacts() as migration:
        contacts = migration.migrate()
    with Orders(archived, closed, opened) as migration:
        orders = migration.migrate()
    with Treatments(archived, closed, opened) as migration:
        treatments = migration.migrate()
    if interactive is True:
        IPython.embed()

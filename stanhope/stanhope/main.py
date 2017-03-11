""" Stanhope Framers Data Migration """
import IPython
import click
from stanhope.migrations import *


@click.command()
@click.option('-i', '--interactive', is_flag=True)
def stanhope(interactive):
    with LegacyCustomers() as migration:
        legacy_customers = migration.migrate()
    with LegacyOrders() as migration:
        legacy_orders = migration.migrate()
    with Accounts() as migration:
        accounts = migration.migrate()
    with Contacts() as migration:
        contacts = migration.migrate()
    with Orders() as migration:
        orders = migration.migrate()
    with Treatments() as migration:
        treatments = migration.migrate()
    if interactive is True:
        IPython.embed()

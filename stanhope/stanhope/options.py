"""
CLI Options
"""
import click


ARCHIVED = click.option('-a', '--archived',
                        flag_value='FrameOrders-Archive',
                        help='Migrate FrameOrders-Archive',
                        is_flag=True)
CLOSED = click.option('-c', '--closed',
                      flag_value='FrameOrders-Closed',
                      help='Migrate FrameOrders-Closed',
                      is_flag=True)
EPOCH = click.option('-[', '--epoch',
                     help='Earliest migrated Order Date')
INTERACTIVE = click.option('-i', '--interactive',
                           is_flag=True,
                           help='Open IPython session after migrations')
JOIN = click.option('-I', '--join',
                    is_flag=True,
                    help='Join Customers/Orders on CustomerNo')
OPENED = click.option('-o', '--opened',
                      flag_value='FrameOrders-Working',
                      help='Migrate FrameOrders-Working',
                      is_flag=True)

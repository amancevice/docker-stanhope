"""
CLI Options
"""
import click


ARCHIVED = click.option('-a', '--archived',
                        flag_value='FrameOrders-Archive',
                        is_flag=True)
CLOSED = click.option('-c', '--closed',
                      flag_value='FrameOrders-Closed',
                      is_flag=True)
INTERACTIVE = click.option('-i', '--interactive', is_flag=True)
OPENED = click.option('-o', '--opened',
                      flag_value='FrameOrders-Working',
                      is_flag=True)

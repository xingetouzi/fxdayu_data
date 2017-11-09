import click
from functools import partial


HELP = "YYYY-MM-DD"
START = click.Option(("-s", "--start"), default=None, help=HELP)
END = click.Option(("-e", "--end"), default=None, help=HELP)
LENGTH = click.Option(("-l", "--length"), default=None, type=click.INT, help=HELP)


def command(timerange, daily):
    group = click.Group(
        commands={
            "range": click.Command("range", callback=timerange,
                                   params=[START, END, LENGTH],
                                   short_help="Calculate and save target position in specific time range."),
            "daily": click.Command("daily", callback=daily,
                                   short_help="Calculate and save most current target position")
        }
    )
    group()


def target_write(external=None, general=None, shift=10):
    def writer_wrap(calculate):
        from fxdayu_data.costume import generate
        from datetime import datetime, timedelta

        def write(df):
            import pandas as pd

            target = generate(general, external).target
            if not isinstance(df, pd.DataFrame):
                raise TypeError("Type of data should be pandas.DataFrame")

            for time, series in df.iterrows():
                target.set(time.strftime("%Y-%m-%d"), series.dropna().to_dict())

        def time_range(start=None, end=None, length=None):
            data = calculate(start, end, length)
            write(data)

        def daily():
            time_range(start=datetime.now()-timedelta(shift))

        return command(time_range, daily)

    return writer_wrap
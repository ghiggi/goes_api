#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 12:13:52 2022

@author: ghiggi
"""
import datetime
import pandas as pd


def _dt_to_year_doy_hour(dt):
    year = dt.strftime("%Y")  # year
    day_of_year = dt.strftime("%j")  # day of year in julian format
    hour = dt.strftime("%H")  # 2-digit hour format
    return year, day_of_year, hour


def get_end_of_day(time):
    time_end_of_day = time + datetime.timedelta(days=1)
    time_end_of_day = time_end_of_day.replace(hour=0, minute=0, second=0)
    return time_end_of_day


def get_start_of_day(time):
    time_start_of_day = time
    time_start_of_day = time_start_of_day.replace(hour=0, minute=0, second=0)
    return time_start_of_day


def get_list_daily_time_blocks(start_time, end_time):
    """Return a list of (start_time, end_time) tuple of daily length."""
    # Retrieve timedelta between start_time and end_time
    dt = end_time - start_time
    # If less than a day
    if dt.days == 0:
        return [(start_time, end_time)]
    # Otherwise split into daily blocks (first and last can be shorter)
    end_of_start_time = get_end_of_day(start_time)
    start_of_end_time = get_start_of_day(end_time)
    # Define list of daily blocks
    l_steps = pd.date_range(end_of_start_time, start_of_end_time, freq="1D")
    l_steps = l_steps.to_pydatetime().tolist()
    l_steps.insert(0, start_time)
    l_steps.append(end_time)
    l_daily_blocks = [(l_steps[i], l_steps[i + 1]) for i in range(0, len(l_steps) - 1)]
    return l_daily_blocks

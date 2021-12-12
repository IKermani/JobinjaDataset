import asyncio
import time
import pandas as pd
import jdatetime as jd
from datetime import timedelta
from tqdm import tqdm

from requests_html import HTMLSession, AsyncHTMLSession

# number of workers to run in parallel
WORKERS = 10
# number of pages to scrap
THRESHOLD = 30
# default URL template
URL_TEMPLATE = 'https://jobinja.ir/jobs?page='
DATE_FORMAT = '%Y/%m/%d'

session = HTMLSession()
r = session.get('https://jobinja.ir/jobs')

# get number of pages available
TOTAL_PAGES = int(r.html.find('.paginator > ul:nth-child(1) > li:nth-child(11) > a:nth-child(1)', first=True).text)

# choose smaller between total_pages and threshold for threshold value
THRESHOLD = min(THRESHOLD, TOTAL_PAGES)

print(f'[i] Number of workers {WORKERS}')
print(f'[i] Total pages is {TOTAL_PAGES}')
print(f'[i] Number of pages to be scrapped {THRESHOLD}')

# dataset columns
titles = []
companies = []
locations = []
payments = []
contract_types = []
passed_days_jalali = []

# tqdm for showing progress bar
pbar = tqdm(total=THRESHOLD)


async def worker(job_queue: asyncio.Queue, response_queue: asyncio.Queue):
    """
    this function run asynchronously and GET the pages
    of the provided URL and put() the Response object
    to the responses_queue.
    """
    while True:
        # get() the page number of the page to scrap
        page_number = await job_queue.get()
        # print(f'[i] getting page {page_number}')
        # create an session object which runs asynchronously
        asession = AsyncHTMLSession()
        response = await asession.get(URL_TEMPLATE + page_number)
        response_queue.put_nowait(response)
        job_queue.task_done()
        # print(f'[i] got page {page_number}')

        # update progress bar
        pbar.update(1)


async def parse(res_q: asyncio.Queue):
    """
    parse the Response objects from res_q
    which was populated by worker() function.
    """
    # for progress bar
    pbar = tqdm(total=THRESHOLD)

    # while the is still a result available in the Queue
    while not res_q.empty():
        res = await res_q.get()

        # extract job title from the HTML
        html_titles = res.html.find('a.c-jobListView__titleLink')
        titles.extend([title.text for title in html_titles])

        # extract passed days from the HTML
        passed_days_list = res.html.find('span.c-jobListView__passedDays')
        passed_days_list = [passed_days.text for passed_days in passed_days_list]

        # the following snippet convert passed days
        # to Jalali Date format
        for date_info in passed_days_list:
            try:
                # if there was an digit in the extracted
                # passed days text, it will be used as
                # timedelta and will be subtracted from date.now()
                # the result will be the job post date
                passed = int(''.join(filter(str.isdigit, date_info)))
                date = jd.datetime.now() - timedelta(days=passed)
            except ValueError:
                # if there was no digit in the extracted text
                # it means that the job posting date is today
                date = jd.datetime.now()

            passed_days_jalali.append(date.strftime(DATE_FORMAT))

        # extract job details section and find other
        # features in there
        sub_res = res.html.find('ul.o-listView__itemComplementInfo')
        for job_desc in sub_res:
            company, location, payment, contract_type = job_desc.find('span')
            companies.append(company.text)
            locations.append(location.text)
            payments.append(payment.text)
            contract_types.append(contract_type.text)

        # update progressbar
        pbar.update(1)

    # close the progressbar
    pbar.close()


async def main():
    # create FIFO Queue for both jobs to be done
    # and the result of the jobs
    job_queue = asyncio.Queue()
    response_queue = asyncio.Queue()

    # create all page numbers that need to be scraped
    for _ in range(THRESHOLD):
        job_queue.put_nowait(str(_ + 1))  # to start from 1

    # Create WORKER number of worker tasks to process the queue concurrently.
    tasks = []
    for i in range(WORKERS):
        task = asyncio.create_task(worker(job_queue, response_queue))
        tasks.append(task)

    # Wait until the queue is fully processed.
    started_at = time.monotonic()
    await job_queue.join()
    total_slept_for = time.monotonic() - started_at
    # close progressbar for the worker()
    pbar.close()

    print('\n[i] parsing responses...')
    await parse(response_queue)
    print('[i] parsing done.')

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)


asyncio.run(main())

# list of all features of dataset
all_jobs_list = [titles, companies, locations, payments, contract_types, passed_days_jalali]
column_names = ['titles', 'companies', 'locations', 'payments', 'contract_types', 'date']

# create dataFrame of features
df = pd.DataFrame(dict(zip(column_names, all_jobs_list)))

# The strategy for updating data is that all records have an
# Jalali data timestamp so if we load the old saved csv file
# and append the old dataframe with the new one we have lots
# of duplicate records and some new ones. so if we drop the
# duplicate records then we can have the historical records and
# newly scrapped data.
try:
    # only update the files
    old_df = pd.read_csv('jobinja.csv')
    df.append(old_df)
    df.drop_duplicates()
except FileNotFoundError:
    # the file does not exist so this is the first run
    pass

print('[i] writing to .xlsx and .csv file...')
df.to_excel('jobinja.xlsx')
df.to_csv('jobinja.csv')
print('[i] write completed.')

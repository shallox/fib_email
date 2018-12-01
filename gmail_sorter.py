from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import pandas as panda
import sqlalchemy
import logging
from datetime import datetime


def inbox_scanner(gcon):
    dnt_start = datetime.now()
    """ Indexes mailbox and adds all messages into Sqlite DB and querys the DB
    to ensure the record doesn't exists """
    print('Starting email gather')
    catch_all = get_all_mb(gcon)
    print(catch_all)
    count = len(catch_all)
    inc_dec = count
    email_list = []
    for email_dat in catch_all:
        more_dat = read_metadata(gcon, email_dat['id'])
        final_dat_clean = dat_clean(more_dat)
        email_list.append(final_dat_clean)
        inc_dec -= 1
        print(f'%{100.0 * inc_dec/count} progress {inc_dec} of {count} left')
    email_all = panda.DataFrame(email_list)
    sql_commit(email_all, 'email_all')


def dat_clean(dat):
    """ Clean up data from google API ready for
    SQL db update """
    new_dat = {}
    for ent in dat.keys():
        model = dat[ent]
        if isinstance(model, dict):
            for key, glob in model.items():
                if isinstance(glob, str) or isinstance(glob, int):
                    new_dat.update({key: glob})
                elif isinstance(glob, dict):
                    new_dat.update({key: str(glob)})
                else:
                    new_dat.update({key: str(glob)})
        elif isinstance(model, list):
            new_val = []
            for glob in model:
                new_val.append(glob)
            new_dat.update({ent: str(' '.join(new_val))})
        else:
            new_dat.update({ent: model})
    return new_dat


def get_all_mb(gcon):
    """ Grab all emails from mailbox """
    response = gcon.users().messages().list(userId='me').execute()
    email = []
    if 'messages' in response:
        email.extend(response['messages'])
    while 'nextPageToken' in response:
        page_token = response['nextPageToken']
        response = gcon.users().messages().list(userId='me', pageToken=page_token).execute()
        if response['resultSizeEstimate'] is 0:
            break
        email.extend(response['messages'])
    return email


def read_metadata(gcon, id):
    """ Grabs all info on given message ID """
    email_dat = gcon.users().messages().get(userId='me', id=id).execute()
    return email_dat


def sql_commit(df, tb):
    """" Takes DataFrame, Table name and Column names then creates
     a table if doesn't exist and finally appends a DataFrame into
     the newly minted DB """
    tbmaker = sqlalchemy.create_engine('sqlite:///mb.db', echo=False)
    df.to_sql(tb, tbmaker, if_exists='append', index=False)
    print('Done')


def sql_read(query_str, vals):
    """ Runs a query via Sqlite, will accept
     values in query if vals is not set to None
    """
    conn = sqlite3.connect('mb.db')
    if vals is None:
        dat_sel = conn.execute(query_str)
    else:
        dat_sel = conn.execute(query_str, vals)
    return dat_sel.fetchall()


def auth_to_g():
    """ Authenticates to gmail server with API key """
    con_url = 'https://www.googleapis.com/auth/gmail.metadata'
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', con_url)
        creds = tools.run_flow(flow, store)
    return build('gmail', 'v1', http=creds.authorize(Http()))


if __name__ == '__main__':
    logging.basicConfig(filename='run.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    gcon = auth_to_g()
    inbox_scanner(gcon)
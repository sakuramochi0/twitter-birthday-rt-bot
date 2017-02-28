#!/usr/bin/env python3
import datetime
import re
import argparse
import csv
import os
import fcntl

import yaml
import tweepy
from dateutil.parser import parse
from get_mongo_client import get_mongo_client
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from get_tweepy import get_api

def inform():
    bd = get_birthday()
    year = datetime.datetime.now().year
    for name, chara in bd.items():
        # remove pripara charas temporally
        if chara['works'] == ['pripara']:
            continue
        if not chara['date']:
            continue

        try:
            date = chara['date'].replace(year=year)
        except ValueError:
            # うるう年以外の2月29日をスキップ
            continue
        date_time = convert_to_datetime(date)
        delta = date_time - datetime.datetime.now()
        hours = round(delta.days * 24 + delta.seconds / 3600)
        days = round(hours / 24)
        weeks = round(days / 7)

        # print('*', ' / '.join(map(str, [name, date, date_time, delta, hours, days, weeks])))
        printed = True

        # just on birthday
        if hours == 0:
            status = ('今日{date.month}月{date.day}日は、'
                      '{works}の{name}の誕生日です！'
                      ' {tag} でお祝いしましょう！'
            ).format(
                date=date,
                works=get_works_str(chara['works']),
                name=name,
                tag=chara['tags'][0].format(name=name, year=year),
            )
            print(status)
            api.update_status(status=status)
        # < 1day
        # elif 0 < hours <= 24:
        #     status = ('{name}の誕生日まで、あと{hours}時間です！'
        #               ' {tag}'
        #     ).format(
        #         date=date,
        #         name=name,
        #         hours=hours,
        #         tag=chara['tags'][0].format(name=name, year=year),
        #     )
        #     print(status)
        # # < 1 weeks
        # elif 0 < days <= 7 and hours // 24 == 0:
        #     status = ('{date.month}月{date.day}日の'
        #               '{works}の{name}の誕生日まで、'
        #               'あと{days}日です！'
        #               ' {tag}'
        #     ).format(
        #         date=date,
        #         works=get_works_str(chara['works']),
        #         name=name,
        #         days=days,
        #         tag=chara['tags'][0].format(name=name, year=year),
        #     )
        #     print(status)
        # # <= 1 month
        # elif 0 < weeks <= 4 and days // 7 == 0 and hours // 24 == 0:
        #     status = ('{date.month}月{date.day}日の'
        #               '{works}の{name}の誕生日まで、'
        #               'あと{days}日です！'
        #               ' {tag}'
        #     ).format(
        #         date=date,
        #         works=get_works_str(chara['works']),
        #         name=name,
        #         days=days,
        #         tag=chara['tags'][0].format(name=name, year=year),
        #     )
        #     print(status)

        # else:
        #     pass

def convert_to_datetime(date):
    return datetime.datetime.fromordinal(date.toordinal())
        
def get_works_str(works):
    ws = []
    for work in works:
        if work == 'AD':
            ws.append('プリティーリズム・オーロラドリーム')
        elif work == 'DMF':
            ws.append('プリティーリズム・ディアマイフューチャー')
        elif work == 'RL':
            ws.append('プリティーリズム・レインボーライブ')
        elif work == 'kinpri':
            ws.append('KING OF PRISM by PrettyRhythm')
        elif work == 'pripara':
            ws.append('プリパラ')
    text = '『{}』' * len(ws)
    return text.format(*ws)

def get_all_tweets():
    bd = get_birthday()
    year = datetime.datetime.now().year
    ts = []
    
    for name, chara in bd.items():
        tags = [tag.format(
            name=name,
            year=year,
        ) for tag in chara['tags']]
        q = make_search_query(tags)
        for t in api.search(q=q, count=200):
            ts.append(t)
    return ts

def get_birthday():
    with open('birthday.yaml') as f:
        bd = yaml.load(f)
    return bd

def make_search_query(tags):
    return ' OR '.join(tags) + ' -RT'

def get_ignores():
    """ignoresファイルからignore_usersを取得する。"""
    with open('ignores.yaml') as f:
        ignores = yaml.load(f)
    return ignores
    
def set_ignores(ignores):
    with open('ignores.yaml', 'w') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yaml.dump(ignores, f, allow_unicode=True)

def add_ignore_users(users):
    """ignoresファイルにusersを追加する。"""
    ignores = get_ignores()
    for user in users:
        u = api.get_user(screen_name=user)
        ignores['deny_users'].append(user)
        ignores['deny_users'].append(u.id)
    set_ignores(ignores)
    
def remove_ignore_users(users):
    """ignoresファイルからuserを削除する。"""
    ignores = get_ignores()
    for user in users:
        u = api.get_user(screen_name=user)
        try:
            ignores['deny_users'].remove(user)
            ignores['deny_users'].remove(u.id)
        except ValueError as e:
            pass
    set_ignores(ignores)
    
def convert_date_to_datetime(date):
    """Convert Date object to Datetime object."""
    return datetime.datetime.fromordinal(date.toordinal())

def make_doc(t):
    """tweepy.Statusから、DBに記録するためのdictを作る。"""
    doc = {
        '_id': t.id,
        't': t._json,
        'meta': {
            'retweeted': False,
            # GMTを日本時間に直す(+9時間)
            'time': t.created_at + datetime.timedelta(hours=9),
        },
    }
    return doc

def is_not_ignore_user(t):
    """ツイートが無視対象ユーザーでないことを判定する。"""
    ignores = get_ignores()
    ignore_users = ignores['ignore_users']
    deny_users = ignores['deny_users']

    if type(t) is dict:
        sn = t['t']['user']['screen_name']
        id =  t['t']['user']['id']
    elif type(t) is tweepy.Status:
        sn = t.user.screen_name
        id = t.user.id
    else:
        raise TypeError('t is neither MongoDB tweet document nor tweepy.Status')

    return sn not in ignore_users \
        and id not in ignore_users \
        and sn not in deny_users \
        and id not in deny_users

def is_not_ignore_keyword(t):
    ignores = get_ignores()
    ignore_keywords = ignores['ignore_keywords']
    if type(t) is dict:
        text = t['t']['text']
    elif type(t) is tweepy.Status:
        text = t.text
    else:
        raise TypeError('t is neither MongoDB tweet document nor tweepy.Status')

    return not any(filter(lambda kw: kw in text, ignore_keywords))

def get_all_tweet_by_search(tag):
    """古い順に並べたツイートリストを返す。"""
    ts = (tweepy.Cursor(
        api.search,
        q='{tag} -RT'.format(tag=tag),
        count=200
    ).items())
    ts = list(reversed(convert_new_payload(ts)))
    return ts

def convert_new_payload(ts):
    if type(ts) is tweepy.Status:
        ts = [ts]
    ts = list(map(_convert_new_payload, ts))
    return ts

def _convert_new_payload(t):
    t.text = t._json['text'] = t.full_text
    return t

def check_replies():
    ts = api.mentions_timeline(tweet_mode='extended')
    ts = convert_new_payload(ts)
    for t in reversed(ts):
        # 処理済みのリプライは無視する
        if replies.find({'_id': t.id}).count():
            continue
        # RT拒否のリプライ
        if re.search(r'(rt|リツイート)しないで', t.text.lower()):
            # 無視リストに登録
            add_ignore_users([t.user.screen_name])
            doc = make_doc(t)
            replies.update_one({'_id': doc['_id']}, {'$set': doc}, upsert=True)
            # 登録完了を知らせる返信
            status = ('【自動リプライ】{name}(@{sn})さんをRT無視リストに登録しました。'
                      'ご迷惑をおかけしてごめんなさい。(._.)'
                      '(再びRTされたい場合は「RTして」とリプライしてください)'
            ).format(name=t.user.name, sn=t.user.screen_name)
            api.update_status(
                status=status,
                auto_populate_reply_metadata=True,
                in_reply_to_status_id=t.id,
            )
        # RT拒否解除のリプライ
        elif re.search(r'(rt|リツイート)して', t.text.lower()):
            # 無視リストに登録
            remove_ignore_users([t.user.screen_name])
            doc = make_doc(t)
            replies.update_one({'_id': doc['_id']}, {'$set': doc}, upsert=True)
            # 登録完了を知らせる返信
            status = ('【自動リプライ】{name}(@{sn})さんをRT無視リストから外しました。'
                      'またRTするようになります。'
            ).format(name=t.user.name, sn=t.user.screen_name)
            api.update_status(
                status=status,
                auto_populate_reply_metadata=True,
                in_reply_to_status_id=t.id,
            )

def fetch_tos():
    # @tosを使った手動RT
    ts = api.user_timeline(screen_name=api.auth.username, count=200, tweet_mode='extended')
    ts = convert_new_payload(ts)
    for t in ts:
        if is_tos(t) and t.user.screen_name == api.auth.username:
            print('if')
            ids = []
            urls = [url['expanded_url'] for url in t.entities['urls']]
            print('urls', urls)
            for url in urls:
                m = re.search(r'(?:https?://.+/)?(\d+)', url)
                if m:
                    ids.append(m.group(1))
            print('ids', ids)
            if ids:
                retweet(ids=ids)
            t.destroy()

def is_tos(t):
    return 'TOS' in [u['name'].upper() for u in t.entities['user_mentions']]

def retweet(ids=None):
    """実際にリツイートを行う関数。"""
    if ids is None:
        tags = get_today_tags()
        if not tags:
            return
        q = make_search_query(tags)
        ts = reversed(api.search(q=q, count=200))
    else:
        ts = api.statuses_lookup(','.join(ids))
    for t in ts:
        # DBにあるものはスキップする
        if tws.find({'_id': t.id, 'meta.retweeted': True}).count():
            continue
        # 無視ユーザー/キーワードを除外する
        if is_not_ignore_user(t) and is_not_ignore_keyword(t):
            try:
                # ドキュメントを作って、リツイートして、DBに登録
                doc = make_doc(t)
                tws.update_one({'_id': doc['_id']}, {'$set': doc}, upsert=True)
                api.retweet(doc['_id'])
                tws.update_one({'_id': doc['_id']}, {'$set': {'meta.retweeted': True}})
            except tweepy.TweepError as e:
                print('e:', e)
                print(t._json)
                # 削除されている(144)か鍵がかかっていた(328)場合は、エラーを記録して終わり
                if e.api_code == 144 or e.api_code == 328:
                    tws.update_one({'_id': doc['_id']}, {'$set': {'meta.error': e.reason}})
                # すでにRTしていた(327)または同じRTで重複ツイートになった(187)場合は
                # テキストは、リツイート済みフラグを立てる
                elif e.api_code == 327 or e.api_code == 187:
                    tws.update_one({'_id': doc['_id']}, {'$set': {'meta.retweeted': True}})
                else:
                    raise

def get_today_tags():
    bd = get_birthday()
    year = datetime.datetime.now().year
    tags = []
    for name, chara in bd.items():
        if not any([target_work in chara['works'] for target_work in args.target_works]):
            continue
        if not chara['date']:
            continue
        try:
            date = chara['date'].replace(year=year)
        except ValueError:
            # うるう年以外の2月29日をスキップ
            continue
        if date - datetime.timedelta(days=3) <= \
           datetime.date.today() <= \
           date + datetime.timedelta(days=7):
            for tag in chara['tags']:
                tags.append(tag.format(name=name, year=year))
    return tags
                
def get_date(doc):
    """Get datetime object of the date when tweet doc is published."""
    date = doc['meta']['time'].date()
    date = convert_date_to_datetime(date)
    return date

def convert_chara_dict(name, c):
    c['name'] = name
    c['year'] = c['year'] and '{}年'.format(c['year'])
    c['date'] = c['date'] and '{dt.month}月{dt.day}日'.format(dt=c['date'])
    c['tags'] = ' / '.join(c['tags']).format(name=name, year='{year}')
    c['works'] = ' / '.join(c['works'])
    return c

def get_header():
    return [
        'works',
        'name', 'name_en', 'name_ko',
        'year', 'date', 'tags', 'note',
    ]

def convert_birthday_to_csv():
    with open('birthday.yaml') as f:
        birthday = yaml.load(f)
    with open('birthday.csv', 'w') as f:
        header = get_header()
        writer = csv.DictWriter(f, header)
        writer.writeheader()
        for name, chara in sorted(birthday.items(), key=lambda x: x[1]['works'][0]):
            chara = convert_chara_dict(name, chara)
            writer.writerow(chara)

def get_gspread():
    scope = ['https://spreadsheets.google.com/feeds']
    creds_path = os.path.join('google-sakuramochi-service-account.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    gc = gspread.authorize(creds)
    return gc
            
def update_birthday_spreadsheet():
    gc = get_gspread()
    # 「プリリズ・プリパラ誕生日リスト」のIDを使ってスプレッドシートを取得
    sh = gc.open_by_key('11f0i3JZE9GkVrCHNLPhcqH_9OGFyZWJddgyub3OYn9o').get_worksheet(0)
    with open('birthday.csv') as f:
        header = get_header()
        reader = csv.reader(f)
        row_num = 500 # 最大行数
        col_num = len(header)
        cells = sh.range('A1:{}'.format(
            sh.get_addr_int(row_num, col_num)
        ))
        cell_num = 0
        for row in reader:
            for val in row:
                cells[cell_num].value = val
                cell_num += 1
        sh.update_cells(cells)
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('account')
    parser.add_argument('command', choices=[
        'inform',
        'retweet',
        'fetch_tos',
        'check_replies',
        'convert_birthday_to_csv',
        'update_birthday_spreadsheet',
        'add_ignore_users',
        'remove_ignore_users',
    ])
    parser.add_argument('--users', '-u', nargs='+')
    parser.add_argument('--target_works', nargs='+')
    parser.add_argument('--ids', nargs='+')
    args = parser.parse_args()

    api = get_api(args.account)
    tws = get_mongo_client()[api.auth.username].tweets
    replies = get_mongo_client()[api.auth.username].replies
    
    if args.command == 'inform':
        inform()
    elif args.command == 'retweet':
        if args.ids:
            retweet(args.ids)
        else:
            retweet()
    elif args.command == 'fetch_tos':
        fetch_tos()
    elif args.command == 'check_replies':
        check_replies()
    elif args.command == 'convert_birthday_to_csv':
        convert_birthday_to_csv()
    elif args.command == 'update_birthday_spreadsheet':
        update_birthday_spreadsheet()
    elif args.command == 'add_ignore_users':
        add_ignore_users(args.users)
    elif args.command == 'remove_ignore_users':
        remove_ignore_users(args.users)
            

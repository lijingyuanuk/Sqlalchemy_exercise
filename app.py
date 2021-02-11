from flask import Flask, request
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from models import *
from datetime import datetime
import json
import logging

app = Flask(__name__)
engine = create_engine('sqlite:///db.sqlite3', echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)
logging.basicConfig(filename='api.log', level=logging.DEBUG)


def add_new_event(message):
    message_id = session.query(Message).filter(Message.id == message.get('id')).scalar()
    event_id = session.query(Event).filter(Event.id == message.get('event').get('id')).scalar()
    if message_id is None and event_id is None:
        try:
            sport_id = session.query(Sport).filter(Sport.id == message.get('event').get('sport').get('id')).scalar()
            if sport_id is None:
                sport = message.get('event').get('sport')
                new_sport = Sport(id=sport.get('id'), name=sport.get('name'))
                session.add(new_sport)

            market_id = session.query(Market).filter(
                Market.id == message.get('event').get('markets')[0].get('id')).scalar()
            if market_id is None:
                market = message.get('event').get('markets')[0]
                selections = market.get('selections')
                for i in selections:
                    selection_id = session.query(Selection).filter(Selection.id == i.get('id')).scalar()
                    if selection_id is None:
                        new_selection = Selection(id=i.get('id'), name=i.get('name'))
                        session.add(new_selection)

                    odd = session.query(Odd).filter(
                        and_(Odd.market_id == market.get('id'), Odd.selection_id == i.get('id'))).scalar()
                    if odd is None:
                        new_odds = Odd(market_id=market.get('id'), selection_id=i.get('id'), odd=i.get('odds'))
                        session.add(new_odds)

                market = message.get('event').get('markets')[0]
                new_market = Market(id=market.get('id'), name=market.get('name'),
                                    sport_id=message.get('event').get('sport').get('id'))
                session.add(new_market)

            event = message.get('event')
            event_id = event.get('id')
            event_name = event.get('name')
            event_time = datetime.strptime(message.get('event').get('startTime'), '%Y-%m-%d %H:%M:%S')
            new_event = Event(id=event_id, name=event_name,
                              url='http://127.0.0.1:5000/api/match/{event_id}'.format(event_id=event_id),
                              start_time=event_time, market_id=market.get('id'))
            session.add(new_event)

            new_message = Message(id=message.get('id'), message_type='NewEvent',
                                  event_id=message.get('event').get('id'))
            session.add(new_message)
            session.commit()
        except Exception as e:
            logging.warning('Failed to add the new event: %s' % e)
    else:
        logging.warning('Cannot add the new event: No valid message or event id')


def update_odds(message):
    market = message.get('event').get('markets')[0]
    market_id = session.query(Market).filter(Market.id == market.get('id')).scalar()
    if market and market_id:
        market_id = market.get('id')
        selections = market.get('selections')
        for i in selections:
            try:
                selection_id = i.get('id')
                odd_value = i.get('odds')
                session.query(Odd). \
                    filter(and_(Odd.selection_id == selection_id, Odd.market_id == market_id)). \
                    update({"odd": odd_value})
                logging.info('An odd with market with Id: {market_id} and selection with Id: {selection_id} is '
                             'updated to {odd_value}.'.format(market_id=market_id, selection_id=selection_id,
                                                              odd_value=odd_value))
            except Exception as e:
                logging.warning('Failed to update an odd: %s' % e)
        session.commit()
    else:
        logging.warning('Cannot update adds: No valid market info')


def encode_match(res):
    event_id = res[0][0]
    event_url = res[0][1]
    event_name = res[0][2]
    event_start_time = res[0][3].strftime('%Y-%m-%d %H:%M:%S')

    sport = res[0][4]
    sport_id = sport.id
    sport_name = sport.name
    market_id = res[0][5]
    market_name = res[0][6]

    selections = list(map(lambda x: dict(id=x[7], name=x[8], odds=x[9]), res))

    match_json = json.dumps({'id': event_id, 'url': event_url, 'name': event_name, 'startTime': event_start_time,
                             'sport': {'id': sport_id, 'name': sport_name},
                             'markets': [{'id': market_id, 'name': market_name, 'selections': selections}]})
    return match_json


def encode_matches(res):
    matches = []
    for i in range(len(res)):
        match_id = res[i][0]
        match_url = res[i][1]
        match_name = res[i][2]
        match_start_time = res[i][3][:19]
        match = {'id': match_id, 'url': match_url, 'name': match_name, 'startTime': match_start_time}
        matches.append(match)

    matches_json = json.dumps(matches)
    return matches_json


@app.route('/api/match/<int:id>', methods=['GET'])
def get_match(id):
    try:
        res = session.query(Event.id, Event.url, Event.name, Event.start_time, Sport, Market.id,
                            Market.name, Selection.id, Selection.name, Odd.odd) \
            .join(Market, Market.id == Event.market_id) \
            .join(Sport, Sport.id == Market.sport_id) \
            .join(Odd, Odd.market_id == Market.id) \
            .join(Selection, Selection.id == Odd.selection_id) \
            .filter(Event.id == id).all()

        if res:
            return encode_match(res)
        else:
            return 'No match with current match id'
    except Exception as e:
        return 'Exception:%s' % e


@app.route('/api/match/', methods=['GET'])
def get_matches():
    try:
        sport = request.args.get('sport')
        ordering = request.args.get('ordering')
        name = request.args.get('name')

        sql = 'select event.id, event.url, event.name, event.startTime from event'

        if sport:
            sql += ''' join market on market.id = event.market_id
                      join sport on sport.id = market.sportId
                                    and lower(sport.name) =  '{sport_name}' '''.format(sport_name=sport.lower())
        if name:
            sql += " where event.name = '{event_name}'".format(event_name=name)

        if ordering:
            sql += ' order by {ordering}'.format(ordering=ordering)
            if ordering.lower() == 'starttime':
                sql += ' desc'

        res = engine.execute(sql).fetchall()

        if res:
            return encode_matches(res)
        else:
            return 'No match on current query conditions'
    except Exception:
        return 'Cannot complete the query'


def validate_date_type(message):
    validation = isinstance(message.get('id'), int) \
                 & isinstance(message.get('message_type'), str)\
                 & isinstance(message.get('event').get('id'), int) \
                 & isinstance(message.get('event').get('name'), str) \
                 & isinstance(message.get('event').get('startTime'), str) \
                 & isinstance(message.get('event').get('sport').get('id'), int) \
                 & isinstance(message.get('event').get('sport').get('name'), str)

    markets = message.get('event').get('markets')
    if markets and validation:
        market = markets[0]
        validation = validation & isinstance(market.get('id'), int) \
                     & isinstance(market.get('name'), str)

        selections = market.get('selections')
        for i in selections:
            validation = validation & isinstance(i.get('id'), int) \
                         & isinstance(i.get('name'), str) \
                         & isinstance(i.get('odds'), float)
    else:
        validation = False
    return validation


@app.route('/api/external_providers', methods=['POST', 'PUT'])
def parse_message():
    if validate_date_type(request.json):
        try:
            message_type = request.json.get('message_type')
            if message_type == 'NewEvent':
                add_new_event(request.json)
                return 'OK'
            elif message_type == 'UpdateOdds':
                update_odds(request.json)
                return 'OK'
            else:
                error_message = 'Invalid message type'
                logging.error(error_message)
                return error_message
        except Exception as e:
            return 'Exception:%s' % e
    else:
        return 'Can not parse the message'


if __name__ == '__main__':
    app.run()

import unittest
import requests
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from models import *
import json
from datetime import datetime

Base = declarative_base()
metadata = Base.metadata

class TestAPI(unittest.TestCase):
    engine = create_engine('sqlite:///db.sqlite3')
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata.create_all(engine)

    def setUp(self):
        self.sport = Sport(id=1, name='golf')

        self.selections = [Selection(id=1, name='A'),
                           Selection(id=2, name='B'),
                           Selection(id=3, name='C')]

        self.market = Market(id=1, name='Winner', sport_id=1)

        self.odds = [Odd(market_id=1, selection_id=1, odd=1.01),
                     Odd(market_id=1, selection_id=2, odd=1.01),
                     Odd(market_id=1, selection_id=3, odd=1.01)]

        self.event = Event(id=1, url='http://example.com/api/match/1', name='A vs B vs C',
                           start_time=datetime(2021, 1, 1, 0, 0, 0),
                           market_id=1)

        self.message = Message(id=1, message_type='NewEvent', event_id=1)

        self.session.add(self.sport)
        self.session.add_all(self.selections)
        self.session.add(self.market)
        self.session.add_all(self.odds)
        self.session.add(self.event)
        self.session.add(self.message)
        self.session.commit()

    def tearDown(self):
        sport = self.session.query(Sport).get(1)
        self.session.delete(sport)

        delete_q = Selection.__table__.delete().where(Selection.id.in_((1, 2, 3)))
        self.session.execute(delete_q)

        market = self.session.query(Market).get(1)
        self.session.delete(market)

        delete_q = Odd.__table__.delete().where(and_(Odd.selection_id.in_((1, 2, 3)), Odd.market_id == 1))
        self.session.execute(delete_q)

        event = self.session.query(Event).get(1)
        self.session.delete(event)

        message = self.session.query(Message).get(1)
        self.session.delete(message)

        self.session.commit()
        self.session.close()

    def test_get_a_match(self):
        url = 'http://127.0.0.1:5000/api/match/1'

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.get(url, headers=headers)

        self.assertEqual(response.status_code, 200)

        expected_response = {
            "id": 1,
            "url": "http://example.com/api/match/1",
            "name": "A vs B vs C",
            "startTime": "2021-01-01 00:00:00",
            "sport": {
                "id": 1,
                "name": "golf"
            },
            "markets": [
                {
                    "id": 1,
                    "name": "Winner",
                    "selections": [
                        {
                            "id": 1,
                            "name": "A",
                            "odds": 1.01
                        },
                        {
                            "id": 2,
                            "name": "B",
                            "odds": 1.01
                        },
                        {
                            "id": 3,
                            "name": "C",
                            "odds": 1.01
                        },
                    ]
                }
            ]
        }

        self.assertEqual(expected_response, json.loads(response.text))

    def test_get_a_match_not_exists(self):

        url = 'http://127.0.0.1:5000/api/match/100'
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.get(url, headers=headers)
        self.assertEqual(response.status_code, 200)

        expected_response = 'No match with current match id'
        self.assertEqual(expected_response, response.text)

    def test_get_matches(self):
        url = 'http://127.0.0.1:5000/api/match/?sport=Golf&ordering=startTime'

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.get(url, headers=headers)

        self.assertEqual(response.status_code, 200)

        expected_response = [
                {
                    "id": 1,
                    "url": "http://example.com/api/match/1",
                    "name": "A vs B vs C",
                    "startTime": "2021-01-01 00:00:00"
                }
            ]

        self.assertEqual(expected_response, json.loads(response.text))

    def test_get_matches_by_name(self):
        url = 'http://127.0.0.1:5000/api/match/?name=A%20vs%20B%20vs%20C'

        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.get(url, headers=headers)

        self.assertEqual(response.status_code, 200)

        expected_response = [
                {
                    "id": 1,
                    "url": "http://example.com/api/match/1",
                    "name": "A vs B vs C",
                    "startTime": "2021-01-01 00:00:00"
                }
            ]

        self.assertEqual(expected_response, json.loads(response.text))

    def test_post_new_event(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 2,
            "message_type": "NewEvent",
            "event": {
                "id": 2,
                "name": "D vs E",
                "startTime": "2021-01-02 00:00:00",
                "sport": {
                    "id": 221,
                    "name": "Football"
                },
                "markets": [
                    {
                        "id": 2,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 4,
                                "name": "D",
                                "odds": 1.01
                            },
                            {
                                "id": 5,
                                "name": "E",
                                "odds": 1.01
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.post(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

        count = self.session.query(Message).filter(Message.id == 2).count()
        self.assertEqual(1, count)

        message = self.session.query(Message.id, Message.message_type, Message.event_id).filter(Message.id == 2).first()
        self.assertEqual((2, 'NewEvent', 2), message)

        event = self.session.query(Event.id, Event.name, Event.url, Event.start_time, Event.market_id).filter(Event.id == 2).first()
        self.assertEqual((2, 'D vs E', 'http://127.0.0.1:5000/api/match/2', datetime(2021, 1, 2, 0, 0), 2), event)

        market = self.session.query(Market.id, Market.name, Market.sport_id).filter(Market.id == 2).first()
        self.assertEqual((2, 'Winner', 221), market)

        selection = self.session.query(Selection.id, Selection.name).filter(Selection.id.in_((4, 5))).all()
        self.assertListEqual([(4, 'D'), (5, 'E')], selection)

        odd = self.session.query(Odd.market_id, Odd.selection_id, Odd.odd).filter(and_(Odd.selection_id.in_((4, 5)), Odd.market_id == 2)).all()
        self.assertListEqual([(2, 4, 1.01), (2, 5, 1.01)], odd)

        # delete posted testing data
        delete_q = Selection.__table__.delete().where(Selection.id.in_((4, 5)))
        self.session.execute(delete_q)

        market = self.session.query(Market).get(2)
        self.session.delete(market)

        delete_q = Odd.__table__.delete().where(and_(Odd.selection_id.in_((4, 5)), Odd.market_id == 2))
        self.session.execute(delete_q)

        event = self.session.query(Event).get(2)
        self.session.delete(event)

        message = self.session.query(Message).get(2)
        self.session.delete(message)

        self.session.commit()

    def test_post_new_event_with_new_sport(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 2,
            "message_type": "NewEvent",
            "event": {
                "id": 3,
                "name": "D vs E",
                "startTime": "2021-01-03 00:00:00",
                "sport": {
                    "id": 100,
                    "name": "Rugby"
                },
                "markets": [
                    {
                        "id": 3,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 6,
                                "name": "F",
                                "odds": 1.01
                            },
                            {
                                "id": 7,
                                "name": "G",
                                "odds": 1.01
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.post(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

        count = self.session.query(Message).filter(Message.id == 2).count()
        self.assertEqual(1, count)

        event = self.session.query(Message.id, Message.message_type, Message.event_id).filter(Message.id == 2).first()
        self.assertEqual(event, (2, 'NewEvent', 3))

        sport = self.session.query(Sport.id, Sport.name).filter(Sport.id == 100).first()
        self.assertEqual(sport, (100, 'Rugby'))

        # delete posted testing data
        delete_q = Selection.__table__.delete().where(Selection.id.in_((6, 7)))
        self.session.execute(delete_q)

        sport = self.session.query(Sport).get(100)
        self.session.delete(sport)

        market = self.session.query(Market).get(3)
        self.session.delete(market)

        delete_q = Odd.__table__.delete().where(and_(Odd.selection_id.in_((6, 7)), Odd.market_id == 3))
        self.session.execute(delete_q)

        event = self.session.query(Event).get(3)
        self.session.delete(event)

        message = self.session.query(Message).get(2)
        self.session.delete(message)

        self.session.commit()

    def test_post_duplicate_message(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 1,
            "message_type": "NewEvent",
            "event": {
                "id": 1,
                "name": "A vs B vs C",
                "startTime": "2021-01-01 00:00:00",
                "sport": {
                    "id": 1,
                    "name": "Golf"
                },
                "markets": [
                    {
                        "id": 1,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 1,
                                "name": "A",
                                "odds": 1.01
                            },
                            {
                                "id": 2,
                                "name": "B",
                                "odds": 1.01
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.post(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

        count = self.session.query(Message).filter(Message.id == 1).count()
        self.assertEqual(1, count)

    def test_put_update_odds(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 1,
            "message_type": "UpdateOdds",
            "event": {
                "id": 1,
                "name": "A vs B vs C",
                "startTime": "2021-01-01 00:00:00",
                "sport": {
                    "id": 1,
                    "name": "Golf"
                },
                "markets": [
                    {
                        "id": 1,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 1,
                                "name": "A",
                                "odds": 10.00
                            },
                            {
                                "id": 2,
                                "name": "B",
                                "odds": 5.55
                            },
                            {
                                "id": 3,
                                "name": "C",
                                "odds": 5.55
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.put(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

        res = self.session.query(Odd.odd). \
            filter(and_(Odd.selection_id.in_((1, 2, 3)), Odd.market_id == 1)) \
            .order_by(Odd.selection_id).all()

        self.assertListEqual(res, [(10.0,), (5.55,), (5.55,)])

    def test_put_update_odds_failed_by_invalid_market_id(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 1,
            "message_type": "UpdateOdds",
            "event": {
                "id": 1,
                "name": "A vs B vs C",
                "startTime": "2021-01-01 00:00:00",
                "sport": {
                    "id": 1,
                    "name": "Golf"
                },
                "markets": [
                    {
                        "id": 999,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 1,
                                "name": "A",
                                "odds": 10.00
                            },
                            {
                                "id": 2,
                                "name": "B",
                                "odds": 5.55
                            },
                            {
                                "id": 3,
                                "name": "C",
                                "odds": 5.55
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.put(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

        res = self.session.query(Odd.odd). \
            filter(and_(Odd.selection_id.in_((1, 2, 3)), Odd.market_id == 1)) \
            .order_by(Odd.selection_id).all()

        self.assertListEqual(res, [(1.01,), (1.01,), (1.01,)])

    def test_put_update_odds_failed_by_invalid_message_type(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 1,
            "message_type": "Not applicable",
            "event": {
                "id": 1,
                "name": "A vs B vs C",
                "startTime": "2021-01-01 00:00:00",
                "sport": {
                    "id": 1,
                    "name": "Golf"
                },
                "markets": [
                    {
                        "id": 1,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 1,
                                "name": "A",
                                "odds": 10.00
                            },
                            {
                                "id": 2,
                                "name": "B",
                                "odds": 5.55
                            },
                            {
                                "id": 3,
                                "name": "C",
                                "odds": 5.55
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.put(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

        self.assertEqual(response.text, 'Invalid message type')

    def test_put_update_odds_failed_by_invalid_odds(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {
            "id": 1,
            "message_type": "UpdateOdds",
            "event": {
                "id": 1,
                "name": "A vs B vs C",
                "startTime": "2021-01-01 00:00:00",
                "sport": {
                    "id": 1,
                    "name": "Golf"
                },
                "markets": [
                    {
                        "id": 1,
                        "name": "Winner",
                        "selections": [
                            {
                                "id": 1,
                                "name": "A",
                                "odds": "not available"
                            },
                            {
                                "id": 2,
                                "name": "B",
                                "odds": "not available"
                            },
                            {
                                "id": 3,
                                "name": "C",
                                "odds": "not available"
                            }
                        ]
                    }
                ]
            }
        }

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.put(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 200)

    def test_put_invalid_payload(self):
        url = 'http://127.0.0.1:5000/api/external_providers'

        test_payload = {}

        data = json.dumps(test_payload)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        response = requests.put(url, headers=headers, data=data)

        self.assertEqual(response.status_code, 500)


if __name__ == '__main__':
    unittest.main()

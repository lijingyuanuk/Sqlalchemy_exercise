from sqlalchemy import Column, String, Integer, UniqueConstraint, ForeignKey, Float, DateTime
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()
metadata = Base.metadata

class Sport(Base):
    __tablename__ = 'sport'

    id = Column('Id', Integer, primary_key=True)
    name = Column('Name', String(255))


class Market(Base):
    __tablename__ = 'market'

    id = Column('Id', Integer, primary_key=True)
    name = Column('Name', String(255))

    sport = relationship("Sport")
    sport_id = Column('SportId', Integer, ForeignKey('sport.Id'))

    selections = association_proxy('market_selection_odd', 'odd')

    UniqueConstraint('Id', 'SportId', name='uc_market_sportId')


class Odd(Base):
    __tablename__ = 'odd'

    market_id = Column('MarketId', Integer, ForeignKey('market.Id'), primary_key=True)
    selection_id = Column('SelectionId', Integer, ForeignKey('selection.Id'), primary_key=True)
    odd = Column('Odd', Float)

    market = relationship(Market,
                          backref=backref("odd")
                          )

    selection = relationship("Selection")

    UniqueConstraint('SelectionId', 'MarketId', name='uc_selectionId_marketId')


class Selection(Base):
    __tablename__ = 'selection'

    id = Column('Id', Integer, primary_key=True)
    name = Column('Name', String(255))


class Event(Base):
    __tablename__ = 'event'

    id = Column('Id', Integer, primary_key=True)
    url = Column('URL', String(255))
    name = Column('Name', String(255))
    start_time = Column('StartTime', DateTime)

    market = relationship("Market")
    market_id = Column(Integer, ForeignKey('market.Id'))

    def __repr__(self):
        return "<Event(id:%s,url:%s,name:%s,startTime:%s)>" % (self.id, self.url, self.name, self.start_time)


class Message(Base):
    __tablename__ = 'message'

    id = Column('Id', Integer, primary_key=True)
    message_type = Column('MessageType', String(255))
    event = relationship("Event")
    event_id = Column('eventId', Integer, ForeignKey('event.Id'))
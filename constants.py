from enum import Enum



class KType(Enum):
    """
    K线类型
    """
    day = 'D'
    week = 'W'
    month = 'M'
    fiveMinute = '5'
    fifthMinute = '15'
    thirtyMinute = '30'
    sixtyMinute = '60'


class DecisionType(Enum):
    """
    决策类型：1 天，2 小时
    """
    day = 1
    hour = 2


class DecisionMake(Enum):
    """
    决策性质
    """
    buy = 'buy'
    sell = 'sell'
    hold = 'hold'

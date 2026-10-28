
class WorkerException(Exception):
    pass


class IgnoreAndAckMessage(WorkerException):
    pass

class CriticalRejectMessage(WorkerException):
    pass


class OutBoxEventRedisException(Exception):
    pass
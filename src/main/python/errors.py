__author__ = "Soumyadip"


class TwitterErrors(Exception):
    def __init__(self, message):
        self.message = message


class UnableToConnectTwitterError(TwitterErrors):
    pass


class PublishErrors(Exception):
    def __init__(self, message):
        self.message = message


class BlankResponseError(PublishErrors):
    pass


class SendMessageError(PublishErrors):
    pass

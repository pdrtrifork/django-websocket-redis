#-*- coding: utf-8 -*-
import sys
import redis
from django.conf import settings
from django.core.handlers.wsgi import WSGIRequest, logger, STATUS_CODE_TEXT
from django.http import HttpResponse, HttpResponseServerError, HttpResponseBadRequest
from django.utils.encoding import force_str
from django.utils.importlib import import_module
from django.utils.functional import SimpleLazyObject
from ws4redis import settings as redis_settings
from ws4redis.exceptions import WebSocketError, HandshakeError, UpgradeRequiredError


class RedisContext(object):
    subscription_protocols = ['subscribe-session', 'subscribe-user', 'subscribe-broadcast']
    publish_protocols = ['publish-session', 'publish-user', 'publish-broadcast']

    def __init__(self):
        self._connection = redis.StrictRedis(host=redis_settings.REDIS_HOST, port=redis_settings.REDIS_PORT, db=0)
        self._subscription = None

    def subscribe_channels(self, request, agreed_protocols):
        def subscribe(prefix):
            key = request.path_info.replace(settings.WEBSOCKET_URL, prefix, 1)
            self._subscription.subscribe(key)

        def publish_on(prefix):
            key = request.path_info.replace(settings.WEBSOCKET_URL, prefix, 1)
            self._publishers.add(key)

        agreed_protocols = [p.lower() for p in agreed_protocols]
        self._subscription = self._connection.pubsub()
        self._publishers = set()

        # subscribe to these Redis channels for outgoing messages
        if 'subscribe-session' in agreed_protocols and request.session:
            subscribe('{0}:'.format(request.session.session_key))
        if 'subscribe-user' in agreed_protocols and request.user:
            subscribe('{0}:'.format(request.user))
        if 'subscribe-broadcast' in agreed_protocols:
            subscribe('broadcast:')

        # publish incoming messages on these Redis channels
        if 'publish-session' in agreed_protocols and request.session:
            publish_on('{0}:'.format(request.session.session_key))
        if 'publish-user' in agreed_protocols and request.user:
            publish_on('{0}:'.format(request.user))
        if 'publish-broadcast' in agreed_protocols:
            publish_on('broadcast:')

    def publish_message(self, message):
        """Publish a message on the subscribed channels in the Redis database"""
        if message:
            for channel in self._publishers:
                self._connection.publish(channel, message)

    def parse_response(self):
        """Parse a message response sent by the Redis database on a subscribed channels"""
        return self._subscription.parse_response()

    def get_file_descriptor(self):
        return self._subscription.connection._sock.fileno()


class WebsocketWSGIServer(object):
    allowed_subprotocols = RedisContext.subscription_protocols + RedisContext.publish_protocols

    def assure_protocol_requirements(self, environ):
        if environ.get('REQUEST_METHOD') != 'GET':
            raise HandshakeError('HTTP method must be a GET')

        if environ.get('SERVER_PROTOCOL') != 'HTTP/1.1':
            raise HandshakeError('HTTP server protocol must be 1.1')

        if environ.get('HTTP_UPGRADE', '').lower() != 'websocket':
            raise HandshakeError('Client does not wish to upgrade to a websocket')

        requested_protocols = [p.strip() for p in environ.get('HTTP_SEC_WEBSOCKET_PROTOCOL', '').split(',')]
        self.agreed_protocols = [p for p in requested_protocols if p.lower() in self.allowed_subprotocols]
        if not self.agreed_protocols:
            raise HandshakeError('Missing Subprotocol, must be one of {0}'.format(', '.join(self.allowed_subprotocols)))

    def process_request(self, request):
        request.session = None
        request.user = None
        if 'django.contrib.sessions.middleware.SessionMiddleware' in settings.MIDDLEWARE_CLASSES:
            engine = import_module(settings.SESSION_ENGINE)
            session_key = request.COOKIES.get(settings.SESSION_COOKIE_NAME, None)
            if session_key:
                request.session = engine.SessionStore(session_key)
                if 'django.contrib.auth.middleware.AuthenticationMiddleware' in settings.MIDDLEWARE_CLASSES:
                    from django.contrib.auth import get_user
                    request.user = SimpleLazyObject(lambda: get_user(request))

    def __call__(self, environ, start_response):
        """ Hijack the main loop from the original thread and listen on events on Redis and Websockets"""
        websocket = None
        redis_context = RedisContext()
        try:
            self.assure_protocol_requirements(environ)
            request = WSGIRequest(environ)
            self.process_request(request)
            websocket = self.upgrade_websocket(environ, start_response)
            print 'agreed_protocols: ', self.agreed_protocols
            redis_context.subscribe_channels(request, self.agreed_protocols)
            websocket_fd = websocket.get_file_descriptor()
            redis_fd = redis_context.get_file_descriptor()
            while websocket and not websocket.closed:
                ready = self.select([websocket_fd, redis_fd], [], [])[0]
                for fd in ready:
                    if fd == websocket_fd:
                        message = websocket.receive()
                        redis_context.publish_message(message)
                    elif fd == redis_fd:
                        response = redis_context.parse_response()
                        if response[0] == 'message':
                            message = response[2]
                            websocket.send(message)
                    else:
                        logger.error('Invalid file descriptor: {0}'.format(fd))
        except WebSocketError, excpt:
            logger.warning('WebSocketError: ', exc_info=sys.exc_info())
            response = HttpResponse(status=1001, content='Websocket Closed')
        except UpgradeRequiredError:
            logger.info('Websocket upgrade required')
            response = HttpResponseBadRequest(status=426, content=excpt)
        except HandshakeError, excpt:
            logger.warning('HandshakeError: ', exc_info=sys.exc_info())
            response = HttpResponseBadRequest(content=excpt)
        except Exception, excpt:
            logger.error('Other Exception: ', exc_info=sys.exc_info())
            response = HttpResponseServerError(content=excpt)
        else:
            response = HttpResponse()
        if websocket:
            websocket.close(code=1001, message='Websocket Closed')
        if hasattr(start_response, 'im_self') and not start_response.im_self.headers_sent:
            status_text = STATUS_CODE_TEXT.get(response.status_code, 'UNKNOWN STATUS CODE')
            status = '{0} {1}'.format(response.status_code, status_text)
            start_response(force_str(status), response._headers.values())
        return response

import redis
from aiohttp import web

class RateConverter(object):
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_handle = redis.Redis(host=redis_host, port=redis_port, db=0)
        self.app = web.Application()
        self.app.router.add_get("/convert", self.convert)
        self.app.router.add_post("/database", self.database)


    async def convert(self, request):
        """Эндпоинт, отвечающий за перевод из одной валюты в другую"""

        from_cur = request.query.get('from')
        to_cur = request.query.get('to')
        amount = float(request.query.get('amount'))

        return web.json_response({
            'from': from_cur,
            'to': to_cur,
            'amount': amount,
            'converted_amount': self.convert_rate(from_cur, to_cur, amount)
        })
    

    async def convert_rate(self, from_cur, to_cur, amount):
        """Переводит из одной валюты в другую"""

        conversion_rate = self.get_convertion_rate(self, from_cur, to_cur)#
        converted_amount = amount * conversion_rate
        return converted_amount
    

    async def get_convertion_rate(self, from_cur, to_cur):
        """Получает коэффициент из базы данных"""

        conversion_rate = self.redis_handle.hget(from_cur, to_cur)
        if not conversion_rate:
            raise web.HTTPNotFound(reason=f'Conversion rate for {from_cur} to {to_cur} is missing')
        
        return float(conversion_rate)


    async def database(self, request):
        """Эндпоинт, отвечающий за внесение данных в базу данных"""

        merge = bool(int(request.query.get('merge', 1)))

        data = await request.json()

        if not data:
            raise web.HTTPBadRequest(reason='No data provided')

        for from_cur, rate_dict in data.items():
            for to_cur, conversion_rate in rate_dict.items():
                self.put_data_to_database(from_cur, to_cur, conversion_rate, merge)

        return web.json_response({'status': 'ok'})
    

    async def put_data_to_database(self, from_cur, to_cur, conversion_rate, merge):
        """Записывает данные в базу данных"""
        
        pipe = self.redis_handle.pipeline()

        if merge:
            # Перезаписываем значения
            pipe.hset(from_cur, to_cur, conversion_rate)
        else:
            # Не перезаписываем значения, только создаём отсутствующие
            pipe.hsetnx(from_cur, to_cur, conversion_rate)

        pipe.execute()


if __name__ == "__main__":
    rate_converter = RateConverter()
    web.run_app(rate_converter.app, port=8000)
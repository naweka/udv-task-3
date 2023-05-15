import unittest
from main import RateConverter

test_db_host, test_db_port = 'localhost', 6379

class RateConverterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.rate_converter = RateConverter(test_db_host, test_db_port)
        self.redis_handle = self.rate_converter.redis_handle

        for key in self.redis_handle.keys():
            self.redis_handle.delete(key)


    async def test_upload_new_data_correctly(self):
        """Проверка успешной загрузки данных"""

        await self.rate_converter.put_data_to_database('X', 'Y', 0.5, True)

        res = await self.rate_converter.get_convertion_rate('X', 'Y')
        self.assertEqual(res, 0.5)


    async def test_overwriting_data_when_merge(self):
        """Проверка перезаписи значений"""

        await self.rate_converter.put_data_to_database('X', 'Y1', 0.5, True)
        await self.rate_converter.put_data_to_database('X', 'Y1', 0.6, True)

        res = await self.rate_converter.get_convertion_rate('X', 'Y1')
        self.assertEqual(res, 0.6)
    

    async def test_not_overwriting_data_when_not_merge(self):
        """Значения не перезаписываются, если merge=0"""
        
        await self.rate_converter.put_data_to_database('X', 'Y2', 0.5, True)
        await self.rate_converter.put_data_to_database('X', 'Y2', 0.6, False)

        res = await self.rate_converter.get_convertion_rate('X', 'Y2')
        self.assertEqual(res, 0.5)

if __name__ == "__main__":
    unittest.main()
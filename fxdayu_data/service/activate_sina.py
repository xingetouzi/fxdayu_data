from fxdayu_data.service.sina import Monitor

if __name__ == '__main__':
    monitor = Monitor("sina_stock.json", db="redis_config.json", log='logging.conf')
    monitor.start()

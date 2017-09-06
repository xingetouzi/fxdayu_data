
class BasicCostume(object):

    def set(self, key, value):
        pass

    def get(self, key):
        pass


class Costume(BasicCostume):

    @property
    def target(self):
        return BasicTarget()


class BasicTarget(BasicCostume):

    def inplace(self, frame):
        pass

    def all(self):
        pass


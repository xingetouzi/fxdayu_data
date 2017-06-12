# coding:utf-8


def value_wrapper(*functions):
    """
    装饰器

    @value_wrapper(func1, func2, func3)
    def func(*args, **kwargs)
        ...
        return something

    ==>

    func3(func2(func1(func(*args, **kwargs)))) <==> func(*args, **kwargs)->func1->func2->func3->result

    :param functions:
    :return:
    """
    def wrapper(method):
        if len(functions) > 1:
            method = value_wrapper(*functions[:-1])(method)

        def func(*args, **kwargs):
            return functions[-1](method(*args, **kwargs))

        return func
    return wrapper

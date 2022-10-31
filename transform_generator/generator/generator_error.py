class GeneratorException(Exception):
    def __init__(self, errors):
        self._errors = errors

    @property
    def errors(self):
        return self._errors

    def __str__(self):
        result = "\t"
        result += "\n\t".join([str(e)for e in self._errors])
        return result


class GeneratorError:
    def __init__(self, row_number, error_message):
        self._row_number = row_number
        self._error_message = error_message

    @property
    def row_number(self):
        return self._row_number

    @property
    def error_message(self):
        return self._error_message

    def __str__(self):
        return 'Row:' + str(self._row_number) + '  Error: ' + self._error_message

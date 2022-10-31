from pydantic import BaseModel, constr, validator
from transform_generator.parser.transform_exp import TransformExp
from transform_generator.parser.transform_exp import get_ast_for_str
from transform_generator.parser.syntax_error_list_exception import SyntaxErrorListException

class TransformExpDTO(BaseModel):
    # NOTE: Order of defined properties is important for validation.
    target_column_name: constr(min_length=1, strict=True)
    expression: constr(min_length=1, strict=True)

    @validator('expression')
    def validate_expression(cls, expression, values):
        """
        Validates that any column name also exists in a table definition.
        @param transformation_expressions:  The value of the field being validated.
        @param values:                      All current object values
        @return:    Returns the field value being validated, or throws an exception on validation failure.
        """
        try:
            get_ast_for_str(expression)
        except SyntaxErrorListException as ex:
            raise ValueError(ex.error_msg_list, f'target_column_name: [{values.get("target_column_name")}]')
        except Exception:
            raise ValueError(f'Unable to parse expression [{expression}].')
        return expression

    @staticmethod
    def from_business_object(src_object: TransformExp, target_column: str):
        return TransformExpDTO(expression=str(src_object), target_column_name=target_column)

    def to_business_object(self):
        # NOTE: AST conversion has already been validated by this point by the custom expression validator.
        return get_ast_for_str(self.expression)

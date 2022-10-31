from antlr4 import InputStream, CommonTokenStream
from functools import cache

from transform_generator.parser.parse_tree_visitor import AstVisitor
from transform_generator.parser.generated.antlr.TransformExpLexer import TransformExpLexer
from transform_generator.parser.generated.antlr.TransformExpParser import TransformExpParser
from transform_generator.parser.syntax_error_list_listener import SyntaxErrorListListener
from transform_generator.parser.syntax_error_list_exception import SyntaxErrorListException
from transform_generator.parser.ast.transform_exp import TransformExp
from transform_generator.parser.visitor.field_dependency_visitor import FieldDependencyVisitor
from typing import Set


@cache
def get_ast_for_str(expr, start_rule="select"):
    exp = InputStream(expr)
    lexer = TransformExpLexer(exp)
    stream = CommonTokenStream(lexer)
    parser = TransformExpParser(stream)

    error_listener = SyntaxErrorListListener()
    lexer.removeErrorListeners()
    lexer.addErrorListener(error_listener)

    parser.removeErrorListeners()
    parser.addErrorListener(error_listener)

    if not hasattr(parser, start_rule):
        raise AttributeError("Parser does not have start rule " + start_rule)

    parse_func = getattr(parser, start_rule)
    tree = parse_func()

    if len(error_listener.error_msg_list) > 0:
        raise SyntaxErrorListException(error_listener.error_msg_list)

    visitor = AstVisitor()
    return visitor.visit(tree)


def get_field_dependencies_for_ast(ast: TransformExp) -> Set[str]:
    visitor = FieldDependencyVisitor()
    ast.accept(visitor)
    if len(visitor.missing_or_invalid_aliases) > 0:
        raise Exception("Missing or invalid aliases: " + str(visitor.missing_or_invalid_aliases))
    return visitor.field_dependencies

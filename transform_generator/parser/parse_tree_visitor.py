from transform_generator.parser.ast.aliased_result_col import AliasedResultCol
from transform_generator.parser.generated.antlr.TransformExpParser import TransformExpParser
from transform_generator.parser.generated.antlr.TransformExpVisitor import TransformExpVisitor
from .ast.between import Between
from .ast.environment_variable import EnvironmentVariable
from .ast.field import Field
from .ast.string_literal import StringLiteral
from .ast.integer_literal import IntegerLiteral
from .ast.decimal_literal import DecimalLiteral
from .ast.null_literal import NullLiteral
from .ast.function_call import FunctionCall
from .ast.join import Join
from .ast.from_clause import FromClause
from .ast.transform_exp import TransformExp
from .ast.bin_op import BinOp
from .ast.cast import Cast
from .ast.case import Case
from .ast.in_clause import InClause
from .ast.window_function_call import WindowFunctionCall
from .ast.paren_exp import ParenExp
from .ast.unary_exp import UnaryExp
from .ast.group_by_clause import GroupByClause


class AstVisitor (TransformExpVisitor):

    def visitAliased_result_column(self, ctx: TransformExpParser.Aliased_result_columnContext):
        result_col = ctx.exp().accept(self)
        if ctx.Identifier():
            alias = ctx.Identifier().getText()
            return AliasedResultCol(result_col, alias)
        else:
            return result_col

    def __init__(self):
        self._table_name_set = set({})

    def visitDecimal_literal(self, ctx: TransformExpParser.Decimal_literalContext):
        return DecimalLiteral(float(ctx.getText()))

    def visitEnvironment_variable(self, ctx: TransformExpParser.Environment_variableContext):
        environment_variable_options = ['processing_date']
        if ctx.Identifier().getText() not in environment_variable_options:
            raise Exception('Environment variables must be in ' + str(environment_variable_options))
        return EnvironmentVariable(ctx.Identifier().getText())

    def visitField_name(self, ctx: TransformExpParser.Field_nameContext):
        field_name = ctx.Identifier().getText()
        field_name = field_name.replace('`', '')
        if ctx.getChildCount() == 1:
            result = Field(field_name)
        else:
            table_name = ctx.table_name().Identifier().getText()
            self._table_name_set.add(table_name)
            result = Field(field_name, table_name)
        return result

    def visitField_name_list(self, ctx: TransformExpParser.Field_name_listContext):
        fields = []
        if ctx.field_name() is not None:
            for field in ctx.field_name():
                fields.append(field.accept(self))
        return fields

    def visitExp_list(self, ctx: TransformExpParser.Exp_listContext):
        exps = []
        if ctx.exp() is not None:
            for param in ctx.exp():
                exps.append(param.accept(self))
        return exps

    def visitFunction_call(self, ctx: TransformExpParser.Function_callContext):
        params = []
        distinct = False
        asterisk = False
        function = ctx.function()
        if ctx.exp_list() is not None:
            params = ctx.exp_list().accept(self)
        if ctx.KW_DISTINCT() is not None:
            distinct = True
        if ctx.ASTERISK() is not None:
            if function.Identifier().getText().lower() == "count":
                # asterisk only to be used with count function
                asterisk = True
            else:
                raise Exception("* argument can only be used with COUNT() function")
        return FunctionCall(function.getText(), params, distinct, asterisk)

    def visitSelect(self, ctx: TransformExpParser.SelectContext):
        distinct = ctx.KW_DISTINCT() is not None
        result_column = ctx.aliased_result_column().accept(self)

        from_clause = None
        if ctx.from_clause() is not None:
            from_clause = ctx.from_clause().accept(self)

        where_clause = None
        if ctx.where_clause() is not None:
            where_clause = ctx.where_clause().accept(self)

        group_by_clause = None
        if ctx.group_by_clause() is not None:
            group_by_clause = ctx.group_by_clause().accept(self)

        return TransformExp(result_column, from_clause, where_clause, group_by_clause, distinct)

    def visitJoin(self, ctx: TransformExpParser.JoinContext):
        operator = None
        alias = None
        if ctx.join_operator() is not None:
            operator = ctx.join_operator().accept(self)
        table_name = ctx.table_name().getText()
        database_name = ctx.database_name.text if ctx.database_name is not None else None
        if ctx.alias() is not None:
            alias = ctx.alias().Identifier().getText()
        condition = None
        if ctx.exp() is not None:
            condition = ctx.exp().accept(self)

        return Join(operator, table_name, condition, database_name, alias)

    def visitFrom_clause(self, ctx: TransformExpParser.From_clauseContext):
        join_ctxs = reversed(ctx.join())
        joins = []
        alias = None
        for join_ctx in join_ctxs:
            joins.insert(0, join_ctx.accept(self))
        database_name = ctx.database_name.text if ctx.database_name is not None else None
        if ctx.alias() is not None:
            alias = ctx.alias().Identifier().getText()
        return FromClause(ctx.table_name().getText(), joins, database_name, alias)

    def visitInteger_literal(self, ctx: TransformExpParser.Integer_literalContext):
        return IntegerLiteral(int(ctx.getText()))

    def visitString_literal(self, ctx: TransformExpParser.String_literalContext):
        return StringLiteral(ctx.getText()[1:-1])

    def visitNull_literal(self, ctx: TransformExpParser.Null_literalContext):
        return NullLiteral()

    def visitExp_bin_op(self, ctx: TransformExpParser.Exp_bin_opContext):
        op = ctx.op.text.upper()

        if op == 'LIKE' and ctx.KW_NOT():
            op = 'NOT LIKE'

        # We support both != and <>, but we always convert to <> as this is the standard SQL operator.
        if op == "!=":
            op = "<>"

        return BinOp(ctx.left.accept(self), op, ctx.right.accept(self))

    def visitExp_is(self, ctx: TransformExpParser.Exp_isContext):
        operator = "IS"
        if ctx.KW_NOT() is not None:
            operator += " NOT"
        return BinOp(ctx.left.accept(self), operator, NullLiteral())

    def visitExp_paren_exp(self, ctx: TransformExpParser.Exp_paren_expContext):
        return ParenExp(ctx.exp().accept(self))

    def visitJoin_operator(self, ctx: TransformExpParser.Join_operatorContext):
        if ctx.getText() == '':
            return None

        result = ""
        if ctx.KW_LEFT() is not None:
            result += "LEFT OUTER"
        elif ctx.KW_RIGHT() is not None:
            result += "RIGHT OUTER"
        elif ctx.KW_FULL() is not None:
            result += "FULL OUTER"
        else:
            if ctx.KW_CROSS() is not None:
                result += "CROSS"
            else:
                if ctx.KW_INNER() is not None:
                    result += "INNER"

        return result

    def visitCast(self, ctx: TransformExpParser.CastContext):
        return Cast(ctx.exp().accept(self), ctx.data_type().getText().upper())

    def visitCase(self, ctx: TransformExpParser.CaseContext):
        exp = ctx.exp().accept(self) if ctx.exp() is not None else None

        when_clauses = []
        for when_ctx in ctx.when_clause():
            when_clauses.append((when_ctx.condition.accept(self), when_ctx.result.accept(self)))

        else_clause = ctx.else_clause().accept(self) if ctx.else_clause() is not None else None

        return Case(exp, when_clauses, else_clause)

    def visitExp_in(self, ctx: TransformExpParser.Exp_inContext):
        exp_list = ctx.exp_list().accept(self) if ctx.exp_list() is not None else []
        not_in = ctx.KW_NOT() is not None
        return InClause(ctx.exp().accept(self), exp_list, not_in)

    def visitGroup_by_clause(self, ctx: TransformExpParser.Group_by_clauseContext):
        exp_list = []
        if ctx.exp_list() is not None:
            exp_list = ctx.exp_list().accept(self)

        having_exp = ctx.exp().accept(self) if ctx.exp() is not None else None
        return GroupByClause(exp_list, having_exp)

    def visitWindow_function(self, ctx: TransformExpParser.Window_functionContext):
        function_call = ctx.function_call().accept(self)
        partition_by = []
        order_by = []
        if ctx.partition_by() is not None:
            partition_by = ctx.partition_by().exp_list().accept(self)
        if ctx.order_by() is not None:
            for exp_direction_pair in ctx.order_by().order_by_exp_list().order_by_exp():
                exp = exp_direction_pair.exp().accept(self)
                if exp_direction_pair.KW_DESC() is not None:
                    direction = 'DESC'
                elif exp_direction_pair.KW_ASC() is not None:
                    direction = 'ASC'
                else:
                    direction = None
                order_by.append((exp, direction))

        return WindowFunctionCall(function_call, partition_by, order_by)

    def visitExp_between(self, ctx: TransformExpParser.Exp_betweenContext):
        test_exp = ctx.test.accept(self)
        begin_exp = ctx.begin_exp.accept(self)
        end_exp = ctx.end_exp.accept(self)

        not_modifier = ctx.KW_NOT() is not None
        return Between(test_exp, begin_exp, end_exp, not_modifier)

    def visitExp_unary(self, ctx: TransformExpParser.Exp_unaryContext):
        return UnaryExp(ctx.op.text, super().visitExp_unary(ctx))

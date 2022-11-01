# Generated from antlr/TransformExp.g4 by ANTLR 4.9.3
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .TransformExpParser import TransformExpParser
else:
    from TransformExpParser import TransformExpParser

# This class defines a complete generic visitor for a parse tree produced by TransformExpParser.

class TransformExpVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by TransformExpParser#select.
    def visitSelect(self, ctx:TransformExpParser.SelectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#aliased_result_column.
    def visitAliased_result_column(self, ctx:TransformExpParser.Aliased_result_columnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#integer_literal.
    def visitInteger_literal(self, ctx:TransformExpParser.Integer_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#decimal_literal.
    def visitDecimal_literal(self, ctx:TransformExpParser.Decimal_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#string_literal.
    def visitString_literal(self, ctx:TransformExpParser.String_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#null_literal.
    def visitNull_literal(self, ctx:TransformExpParser.Null_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#field_name.
    def visitField_name(self, ctx:TransformExpParser.Field_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#table_name.
    def visitTable_name(self, ctx:TransformExpParser.Table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#field_name_list.
    def visitField_name_list(self, ctx:TransformExpParser.Field_name_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_list.
    def visitExp_list(self, ctx:TransformExpParser.Exp_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#function.
    def visitFunction(self, ctx:TransformExpParser.FunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#function_call.
    def visitFunction_call(self, ctx:TransformExpParser.Function_callContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#order_by_exp.
    def visitOrder_by_exp(self, ctx:TransformExpParser.Order_by_expContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#order_by_exp_list.
    def visitOrder_by_exp_list(self, ctx:TransformExpParser.Order_by_exp_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#order_by.
    def visitOrder_by(self, ctx:TransformExpParser.Order_byContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#partition_by.
    def visitPartition_by(self, ctx:TransformExpParser.Partition_byContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#window_function.
    def visitWindow_function(self, ctx:TransformExpParser.Window_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#cast.
    def visitCast(self, ctx:TransformExpParser.CastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#data_type.
    def visitData_type(self, ctx:TransformExpParser.Data_typeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#data_type_scale.
    def visitData_type_scale(self, ctx:TransformExpParser.Data_type_scaleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#data_type_decimal.
    def visitData_type_decimal(self, ctx:TransformExpParser.Data_type_decimalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#case.
    def visitCase(self, ctx:TransformExpParser.CaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#when_clause.
    def visitWhen_clause(self, ctx:TransformExpParser.When_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#else_clause.
    def visitElse_clause(self, ctx:TransformExpParser.Else_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#alias.
    def visitAlias(self, ctx:TransformExpParser.AliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#from_clause.
    def visitFrom_clause(self, ctx:TransformExpParser.From_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#join.
    def visitJoin(self, ctx:TransformExpParser.JoinContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#join_operator.
    def visitJoin_operator(self, ctx:TransformExpParser.Join_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#where_clause.
    def visitWhere_clause(self, ctx:TransformExpParser.Where_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#group_by_clause.
    def visitGroup_by_clause(self, ctx:TransformExpParser.Group_by_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#environment_variable.
    def visitEnvironment_variable(self, ctx:TransformExpParser.Environment_variableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_is.
    def visitExp_is(self, ctx:TransformExpParser.Exp_isContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_paren_exp.
    def visitExp_paren_exp(self, ctx:TransformExpParser.Exp_paren_expContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_null_literal.
    def visitExp_null_literal(self, ctx:TransformExpParser.Exp_null_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_unary.
    def visitExp_unary(self, ctx:TransformExpParser.Exp_unaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_cast.
    def visitExp_cast(self, ctx:TransformExpParser.Exp_castContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_bin_op.
    def visitExp_bin_op(self, ctx:TransformExpParser.Exp_bin_opContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_func_call.
    def visitExp_func_call(self, ctx:TransformExpParser.Exp_func_callContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_in.
    def visitExp_in(self, ctx:TransformExpParser.Exp_inContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_decimal_literal.
    def visitExp_decimal_literal(self, ctx:TransformExpParser.Exp_decimal_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_field_name.
    def visitExp_field_name(self, ctx:TransformExpParser.Exp_field_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_window_function.
    def visitExp_window_function(self, ctx:TransformExpParser.Exp_window_functionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_integer_literal.
    def visitExp_integer_literal(self, ctx:TransformExpParser.Exp_integer_literalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_case.
    def visitExp_case(self, ctx:TransformExpParser.Exp_caseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_between.
    def visitExp_between(self, ctx:TransformExpParser.Exp_betweenContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_environment_variable.
    def visitExp_environment_variable(self, ctx:TransformExpParser.Exp_environment_variableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by TransformExpParser#exp_string_literal.
    def visitExp_string_literal(self, ctx:TransformExpParser.Exp_string_literalContext):
        return self.visitChildren(ctx)



del TransformExpParser
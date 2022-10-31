grammar TransformExp;

/* Lexical Specification */
KW_AND : A N D;
KW_AS : A S;
KW_ASC : A S C;
KW_BETWEEN : B E T W E E N;
KW_BIGINT : B I G I N T;
KW_BY : B Y;
KW_CASE : C A S E;
KW_CAST : C A S T;
KW_CHAR : C H A R;
KW_CROSS : C R O S S;
KW_DATE : D A T E;
KW_DECIMAL : D E C I M A L;
KW_DESC : D E S C;
KW_DISTINCT : D I S T I N C T;
KW_DOUBLE : D O U B L E;
KW_ELSE : E L S E;
KW_END : E N D;
KW_FROM : F R O M;
KW_FULL : F U L L;
KW_GROUP : G R O U P;
KW_INT : I N T;
KW_IN : I N;
KW_INNER : I N N E R;
KW_IS : I S;
KW_JOIN : J O I N;
KW_LEFT : L E F T;
KW_LIKE : L I K E;
KW_NOT : N O T;
KW_NULL : N U L L;
KW_ON : O N;
KW_OR : O R;
KW_ORDER : O R D E R;
KW_OUTER : O U T E R;
KW_OVER : O V E R;
KW_PARTITION : P A R T I T I O N;
KW_RIGHT : R I G H T;
KW_SELECT : S E L E C T;
KW_SMALLINT : S M A L L I N T;
KW_STRING : S T R I N G;
KW_TIMESTAMP : T I M E S T A M P;
KW_TINYINT : T I N Y I N T;
KW_THEN : T H E N;
KW_WHEN : W H E N;
KW_WHERE : W H E R E;
KW_HAVING : H A V I N G;

DOT : '.';
L_PAREN : '(';
R_PAREN : ')';
COMMA : ',';
ASTERISK : '*';

fragment
Letter
    : 'a'..'z' | 'A'..'Z';

fragment
Digit
    :
    '0'..'9';

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\'')+
    ;

NumberLiteral
    : Digit+;

Identifier
    :
    (Letter | Digit | '_')+
    | ( '`' ( ~('`') )* '`')+;

WS  :  (' '|'\r'|'\t'|'\n') -> channel(HIDDEN);


fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];


select : (KW_SELECT)? (KW_DISTINCT)? aliased_result_column (from_clause)? (where_clause)? (group_by_clause)? EOF;

aliased_result_column : exp (KW_AS Identifier)?;

integer_literal: NumberLiteral;
decimal_literal: (NumberLiteral)? DOT NumberLiteral;
string_literal: StringLiteral;
null_literal: KW_NULL;

field_name : (table_name DOT)? Identifier;
table_name : Identifier;
field_name_list : field_name (COMMA field_name)*;

// For function calls and IN
exp_list : exp (COMMA exp)*;

// Function Calls
function : KW_CHAR | Identifier;
function_call : function L_PAREN KW_DISTINCT? (exp_list | ASTERISK)? R_PAREN;

// Window functions
order_by_exp : exp (KW_ASC | KW_DESC)?;
order_by_exp_list : order_by_exp (COMMA order_by_exp)*;
order_by : KW_ORDER KW_BY order_by_exp_list;
partition_by :  KW_PARTITION KW_BY exp_list;
window_function : function_call  KW_OVER L_PAREN partition_by? order_by? R_PAREN;

// Cast
cast : KW_CAST L_PAREN exp KW_AS data_type R_PAREN;
data_type : KW_STRING | KW_DATE | KW_INT | KW_TIMESTAMP | KW_DOUBLE | KW_BIGINT | KW_SMALLINT | KW_TINYINT
    | KW_DECIMAL data_type_decimal? | KW_CHAR data_type_scale?;
data_type_scale : L_PAREN NumberLiteral+ R_PAREN;
data_type_decimal : L_PAREN NumberLiteral COMMA NumberLiteral R_PAREN;

//Case statment
case : KW_CASE exp? when_clause+ (else_clause)? KW_END;
when_clause : KW_WHEN condition=exp KW_THEN result=exp;
else_clause : KW_ELSE exp;

// From Clause
alias : KW_AS? Identifier;
from_clause : KW_FROM (database_name=Identifier DOT)? table_name alias? (join)*;

join : join_operator? KW_JOIN (database_name=Identifier DOT)? table_name alias? (KW_ON exp)?;
join_operator : KW_CROSS | ((KW_LEFT | KW_RIGHT | KW_FULL) KW_OUTER?) | KW_INNER ;

where_clause : KW_WHERE exp;

group_by_clause : KW_GROUP KW_BY exp_list (KW_HAVING exp)?;

environment_variable : ('${' Identifier '}');

exp : field_name      # exp_field_name
    | environment_variable # exp_environment_variable
    | string_literal  # exp_string_literal
    | integer_literal # exp_integer_literal
    | decimal_literal # exp_decimal_literal
    | null_literal    # exp_null_literal
    | function_call   # exp_func_call
    | window_function # exp_window_function
    | case            # exp_case
    | cast            # exp_cast
    | exp (KW_NOT)? KW_IN L_PAREN exp_list R_PAREN # exp_in
    | L_PAREN exp R_PAREN # exp_paren_exp
    | left=exp op=('*' | '/' | '%') right = exp # exp_bin_op
    | left=exp op=('+' | '-') right=exp # exp_bin_op
    | left=exp op=('=' | '<' | '<=' | '>' | '>=' | '!=' | '<>') right=exp #exp_bin_op
    | left=exp (KW_IS (KW_NOT)? KW_NULL) #exp_is
    | op=('-' | KW_NOT) exp #exp_unary
    | left=exp op=KW_AND right=exp # exp_bin_op
    | left=exp op=KW_OR right=exp # exp_bin_op
    | test=exp KW_NOT? KW_BETWEEN begin_exp=exp KW_AND end_exp=exp #exp_between
    | left=exp KW_NOT? op=KW_LIKE right=exp # exp_bin_op;


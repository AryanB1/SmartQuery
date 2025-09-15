grammar SqlParser;

// Parser Rules
statement
    : selectStatement EOF
    ;

selectStatement
    : SELECT selectList 
      FROM tableName 
      (WHERE whereClause)? 
      (GROUP BY groupByList)? 
      (ORDER BY orderByList)? 
      (LIMIT limitClause)?
    ;

selectList
    : selectItem (',' selectItem)*
    ;

selectItem
    : '*'                                    # SelectStar
    | expression (AS? alias)?                # SelectExpression
    ;

expression
    : aggregateFunction                      # AggregateExpression
    | identifier                             # ColumnReference
    | literal                               # LiteralExpression
    | '(' expression ')'                    # ParenthesizedExpression
    | expression operator=(EQ | NE | LT | LE | GT | GE) expression  # ComparisonExpression
    | expression AND expression            # AndExpression
    | expression OR expression             # OrExpression
    | expression IN '(' expressionList ')' # InExpression
    | expression BETWEEN expression AND expression  # BetweenExpression
    | expression LIKE STRING               # LikeExpression
    ;

aggregateFunction
    : COUNT '(' '*' ')'                     # CountStarFunction
    | COUNT '(' identifier ')'              # CountFunction
    | SUM '(' identifier ')'                # SumFunction
    | AVG '(' identifier ')'                # AvgFunction
    | MIN '(' identifier ')'                # MinFunction
    | MAX '(' identifier ')'                # MaxFunction
    ;

expressionList
    : expression (',' expression)*
    ;

whereClause
    : expression
    ;

groupByList
    : identifier (',' identifier)*
    ;

orderByList
    : orderByItem (',' orderByItem)*
    ;

orderByItem
    : identifier (ASC | DESC)?
    ;

limitClause
    : INTEGER
    ;

tableName
    : identifier
    ;

alias
    : identifier
    ;

identifier
    : IDENTIFIER
    ;

literal
    : INTEGER
    | FLOAT
    | STRING
    ;

// Lexer Rules
SELECT : S E L E C T ;
FROM : F R O M ;
WHERE : W H E R E ;
GROUP : G R O U P ;
BY : B Y ;
ORDER : O R D E R ;
LIMIT : L I M I T ;
AS : A S ;
AND : A N D ;
OR : O R ;
IN : I N ;
BETWEEN : B E T W E E N ;
LIKE : L I K E ;
ASC : A S C ;
DESC : D E S C ;

// Aggregate functions
COUNT : C O U N T ;
SUM : S U M ;
AVG : A V G ;
MIN : M I N ;
MAX : M A X ;

// Operators
EQ : '=' ;
NE : '!=' | '<>' ;
LT : '<' ;
LE : '<=' ;
GT : '>' ;
GE : '>=' ;

// Literals
INTEGER : [0-9]+ ;
FLOAT : [0-9]+ '.' [0-9]+ ;
STRING : '\'' (~['\r\n] | '\'\'')* '\'' ;

// Identifiers
IDENTIFIER : [a-zA-Z_][a-zA-Z0-9_]* ;

// Whitespace and comments
WS : [ \t\r\n]+ -> skip ;
LINE_COMMENT : '--' ~[\r\n]* -> skip ;

// Case-insensitive keywords
fragment A : [aA] ;
fragment B : [bB] ;
fragment C : [cC] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment F : [fF] ;
fragment G : [gG] ;
fragment H : [hH] ;
fragment I : [iI] ;
fragment J : [jJ] ;
fragment K : [kK] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment N : [nN] ;
fragment O : [oO] ;
fragment P : [pP] ;
fragment Q : [qQ] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment U : [uU] ;
fragment V : [vV] ;
fragment W : [wW] ;
fragment X : [xX] ;
fragment Y : [yY] ;
fragment Z : [zZ] ;
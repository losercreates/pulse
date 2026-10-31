package cep

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

var (
	ErrSyntax          = errors.New("expression syntax error")
	ErrDivisionByZero  = errors.New("division by zero in expression")
	ErrMissingVariable = errors.New("variable not present in metrics map")
)

// Evaluator implements ExpressionEvaluator.
type Evaluator struct{}

// NewExpressionEvaluator creates an arithmetic expression evaluator.
func NewExpressionEvaluator() *Evaluator {
	return &Evaluator{}
}

// Evaluate evaluates an arithmetic or comparison expression and returns a float64 (1.0 for true, 0.0 for false).
func (e *Evaluator) Evaluate(expression string, values map[string]float64) (float64, error) {
	expr := strings.TrimSpace(expression)
	if expr == "" {
		return 0, ErrSyntax
	}

	// Check if this is a comparison expression (e.g. "a / b < 0.70")
	for _, op := range []string{"<=", ">=", "==", "!=", "<", ">"} {
		idx := strings.Index(expr, op)
		if idx > 0 {
			leftPart := strings.TrimSpace(expr[:idx])
			rightPart := strings.TrimSpace(expr[idx+len(op):])

			leftVal, err := evalArithmetic(leftPart, values)
			if err != nil {
				return 0, err
			}
			rightVal, err := evalArithmetic(rightPart, values)
			if err != nil {
				return 0, err
			}

			var truth bool
			switch op {
			case "<":
				truth = leftVal < rightVal
			case "<=":
				truth = leftVal <= rightVal
			case ">":
				truth = leftVal > rightVal
			case ">=":
				truth = leftVal >= rightVal
			case "==":
				truth = leftVal == rightVal
			case "!=":
				truth = leftVal != rightVal
			}

			if truth {
				return 1.0, nil
			}
			return 0.0, nil
		}
	}

	// Plain arithmetic
	return evalArithmetic(expr, values)
}

// EvaluateCondition returns boolean whether condition holds.
func (e *Evaluator) EvaluateCondition(expression string, values map[string]float64) (bool, error) {
	res, err := e.Evaluate(expression, values)
	if err != nil {
		return false, err
	}
	return res > 0.5, nil
}

// Token types for recursive descent
type tokenType int

const (
	tokNumber tokenType = iota
	tokIdent
	tokPlus
	tokMinus
	tokMul
	tokDiv
	tokLParen
	tokRParen
	tokEOF
)

type token struct {
	typ tokenType
	val string
	num float64
}

type parser struct {
	tokens []token
	pos    int
	values map[string]float64
}

func evalArithmetic(expr string, values map[string]float64) (float64, error) {
	tokens, err := tokenize(expr)
	if err != nil {
		return 0, err
	}
	p := &parser{tokens: tokens, pos: 0, values: values}
	res, err := p.parseExpr()
	if err != nil {
		return 0, err
	}
	if p.peek().typ != tokEOF {
		return 0, fmt.Errorf("%w: unexpected token '%s'", ErrSyntax, p.peek().val)
	}
	return res, nil
}

func tokenize(input string) ([]token, error) {
	var tokens []token
	runes := []rune(input)
	i := 0
	n := len(runes)

	for i < n {
		r := runes[i]
		if unicode.IsSpace(r) {
			i++
			continue
		}

		if r == '+' {
			tokens = append(tokens, token{typ: tokPlus, val: "+"})
			i++
		} else if r == '-' {
			tokens = append(tokens, token{typ: tokMinus, val: "-"})
			i++
		} else if r == '*' {
			tokens = append(tokens, token{typ: tokMul, val: "*"})
			i++
		} else if r == '/' {
			tokens = append(tokens, token{typ: tokDiv, val: "/"})
			i++
		} else if r == '(' {
			tokens = append(tokens, token{typ: tokLParen, val: "("})
			i++
		} else if r == ')' {
			tokens = append(tokens, token{typ: tokRParen, val: ")"})
			i++
		} else if unicode.IsDigit(r) || r == '.' {
			start := i
			for i < n && (unicode.IsDigit(runes[i]) || runes[i] == '.') {
				i++
			}
			numStr := string(runes[start:i])
			f, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid number %s: %w", numStr, err)
			}
			tokens = append(tokens, token{typ: tokNumber, val: numStr, num: f})
		} else if unicode.IsLetter(r) || r == '_' {
			start := i
			for i < n && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			ident := string(runes[start:i])
			tokens = append(tokens, token{typ: tokIdent, val: ident})
		} else {
			return nil, fmt.Errorf("unrecognized character '%c'", r)
		}
	}

	tokens = append(tokens, token{typ: tokEOF})
	return tokens, nil
}

func (p *parser) peek() token {
	if p.pos >= len(p.tokens) {
		return token{typ: tokEOF}
	}
	return p.tokens[p.pos]
}

func (p *parser) next() token {
	tok := p.peek()
	p.pos++
	return tok
}

func (p *parser) parseExpr() (float64, error) {
	return p.parseAddSub()
}

func (p *parser) parseAddSub() (float64, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return 0, err
	}

	for {
		tok := p.peek()
		if tok.typ == tokPlus {
			p.next()
			right, err := p.parseMulDiv()
			if err != nil {
				return 0, err
			}
			left += right
		} else if tok.typ == tokMinus {
			p.next()
			right, err := p.parseMulDiv()
			if err != nil {
				return 0, err
			}
			left -= right
		} else {
			break
		}
	}
	return left, nil
}

func (p *parser) parseMulDiv() (float64, error) {
	left, err := p.parseUnary()
	if err != nil {
		return 0, err
	}

	for {
		tok := p.peek()
		if tok.typ == tokMul {
			p.next()
			right, err := p.parseUnary()
			if err != nil {
				return 0, err
			}
			left *= right
		} else if tok.typ == tokDiv {
			p.next()
			right, err := p.parseUnary()
			if err != nil {
				return 0, err
			}
			if right == 0 {
				return 0, ErrDivisionByZero
			}
			left /= right
		} else {
			break
		}
	}
	return left, nil
}

func (p *parser) parseUnary() (float64, error) {
	tok := p.peek()
	if tok.typ == tokMinus {
		p.next()
		val, err := p.parseUnary()
		if err != nil {
			return 0, err
		}
		return -val, nil
	} else if tok.typ == tokPlus {
		p.next()
		return p.parseUnary()
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (float64, error) {
	tok := p.next()
	switch tok.typ {
	case tokNumber:
		return tok.num, nil
	case tokIdent:
		val, ok := p.values[tok.val]
		if !ok {
			return 0, fmt.Errorf("%w: %s", ErrMissingVariable, tok.val)
		}
		return val, nil
	case tokLParen:
		val, err := p.parseExpr()
		if err != nil {
			return 0, err
		}
		if p.peek().typ != tokRParen {
			return 0, fmt.Errorf("%w: missing closing parenthesis", ErrSyntax)
		}
		p.next()
		return val, nil
	default:
		return 0, fmt.Errorf("%w: unexpected token %s", ErrSyntax, tok.val)
	}
}

func (e *Engine) evaluateExpression(rule CEPRule, reading *telemetryv1.TelemetryReading, zScore float64, devMap map[string]metricEntry) *telemetryv1.AlertEvent {
	// Build map of current values
	values := make(map[string]float64)
	for k, v := range devMap {
		values[k] = v.value
	}

	match, err := e.exprEval.EvaluateCondition(rule.Expression, values)
	if err != nil || !match {
		return nil
	}

	// Cooldown check
	cooldownKey := rule.RuleID + ":" + reading.DeviceId
	if last, ok := e.lastAlertTime[cooldownKey]; ok {
		if reading.TimestampNs-last < int64(rule.Cooldown) {
			return nil
		}
	}
	e.lastAlertTime[cooldownKey] = reading.TimestampNs

	return e.createAlert(rule, reading, zScore, nil)
}

// Ensure interface adherence
var _ ExpressionEvaluator = (*Evaluator)(nil)

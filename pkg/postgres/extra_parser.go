package parser

import "strconv"

func (expr *StrVal)OriginalString()string{
	return expr.s
}

func (expr *NumVal)ParseValue()(interface{}, error){
	intValue, err := strconv.ParseInt(expr.OrigString, 10, 64)
	if err == nil {
		return intValue, nil
	}
	floatValue, err := strconv.ParseFloat(expr.OrigString, 64)
	if err == nil {
		return floatValue, nil
	}
	ddcimal, err := ParseDDecimal(expr.OrigString)
	if err != nil {
		return nil, err
	}
	return ddcimal.Decimal, nil
}

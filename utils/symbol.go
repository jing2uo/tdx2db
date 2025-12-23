package utils

func GenerateSymbol(code string) (string, bool) {
	if len(code) < 2 {
		return code, false
	}

	prefix := code[:2]

	switch prefix {
	case "00", "30":
		return "sz" + code, true
	case "60", "68":
		return "sh" + code, true
	case "92", "87", "83", "43":
		return "bj" + code, true
	default:
		return code, false
	}
}

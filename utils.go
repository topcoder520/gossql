package gossql

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
)

type Tag int

const (
	ColTag Tag = iota
)

func (t Tag) String() string {
	return []string{"col"}[t]
}

func Mapping(m map[string]string, v reflect.Value) error {
	t := v.Type()
	val := v.Elem()
	typ := t.Elem()
	if !val.IsValid() {
		return errors.New("data type error")
	}
	if val.Kind() == reflect.String {
		if !val.CanSet() {
			return errors.New("data can not set")
		}
		for _, v := range m {
			val.SetString(v)
		}
	} else if val.Kind() == reflect.Slice {
		length := len(m)
		newv := reflect.MakeSlice(val.Type(), 0, length)
		val.Set(newv)
		val.SetLen(length)

		keys := make([]string, 0, length)
		for key := range m {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		var index = 0
		for _, Key := range keys {
			value := m[Key]
			k := val.Type().Elem()
			newObj := reflect.New(k)
			newObjVal := newObj.Elem()
			if newObjVal.CanSet() {
				newObjVal.SetString(value)
				if newObjVal.Kind() == reflect.Ptr {
					newObjVal = newObjVal.Elem()
				}
				val.Index(index).Set(newObjVal)
			}
			index++
		}

	} else {
		for i := 0; i < val.NumField(); i++ {
			value := val.Field(i)
			kind := value.Kind()
			colTag := ColTag.String()
			tag := typ.Field(i).Tag.Get(colTag)
			if len(tag) > 0 {
				meta, ok := m[tag]
				if !ok {
					continue
				}
				if !value.CanSet() {
					return errors.New("data can not set")
				}
				if len(meta) == 0 {
					continue
				}
				switch kind {
				case reflect.String:
					value.SetString(meta)
				case reflect.Float32:
					f, err := strconv.ParseFloat(meta, 32)
					if err != nil {
						return err
					}
					value.SetFloat(f)
				case reflect.Float64:
					f, err := strconv.ParseFloat(meta, 64)
					if err != nil {
						return err
					}
					value.SetFloat(f)
				case reflect.Int64:
					integer64, err := strconv.ParseInt(meta, 10, 64)
					if err != nil {
						return err
					}
					value.SetInt(integer64)
				case reflect.Int32:
					integer32, err := strconv.ParseInt(meta, 10, 32)
					if err != nil {
						return err
					}
					value.SetInt(integer32)
				case reflect.Int16:
					integer16, err := strconv.ParseInt(meta, 10, 16)
					if err != nil {
						return err
					}
					value.SetInt(integer16)
				case reflect.Int8:
					integer8, err := strconv.ParseInt(meta, 10, 8)
					if err != nil {
						return err
					}
					value.SetInt(integer8)
				case reflect.Int:
					integer, err := strconv.Atoi(meta)
					if err != nil {
						return err
					}
					value.SetInt(int64(integer))
				case reflect.Bool:
					b, err := strconv.ParseBool(meta)
					if err != nil {
						return err
					}
					value.SetBool(b)
				default:
					return errors.New("data map failed ")
				}
			}
		}
	}
	return nil
}

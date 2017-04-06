package model

type Indices struct {
	Start int
}

type Dimension struct {
	Hierarchy string
	Name      string
	Value     string
}

func (i *Indices) Hierarchy() int {
	return i.Start
}

func (i *Indices) Name() int {
	return i.Start + 1
}

func (i *Indices) Value() int {
	return i.Start + 2
}

package fast

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/aclivo/olap"
)

type storage struct {
	cubes      *cubes
	dimensions *dimensions
	elements   *elements
	cells      *cells
}

// NewStorage creates a new fast storage.
func NewStorage() olap.Storage {
	return &storage{
		cubes:      newCubes(),
		dimensions: newDimensions(),
		elements:   newElements(),
		cells:      newCells(),
	}
}

func (s *storage) AddCube(ctx context.Context, cube olap.Cube) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return s.cubes.addCube(cube)
}

func (s *storage) GetCube(ctx context.Context, name string) (olap.Cube, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return olap.Cube{}, ctx.Err()
	}
	return s.cubes.getCube(name)
}

func (s *storage) AddDimension(ctx context.Context, dim olap.Dimension) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return s.dimensions.addDimension(dim)
}

func (s *storage) GetDimension(ctx context.Context, name string) (olap.Dimension, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return olap.Dimension{}, ctx.Err()
	}
	return s.dimensions.getDimension(name)
}

func (s *storage) AddElement(ctx context.Context, el olap.Element) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return s.elements.addElement(el)
}

func (s *storage) GetElement(ctx context.Context, dim, el string) (olap.Element, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return olap.Element{}, ctx.Err()
	}
	return s.elements.getElement(dim, el)
}

func (s *storage) AddComponent(ctx context.Context, tot, el olap.Element) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return s.elements.addComponent(tot, el)
}

func (s *storage) GetComponent(ctx context.Context, dim, name string) (olap.Element, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return olap.Element{}, ctx.Err()
	}
	return s.elements.getComponent(dim, name)
}

func (s *storage) Children(ctx context.Context, dim, name string) ([]olap.Element, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return []olap.Element{}, ctx.Err()
	}
	return s.elements.children(dim, name)
}

func (s *storage) AddCell(ctx context.Context, cell olap.Cell) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}
	return s.cells.addCell(cell)
}

func (s *storage) GetCell(ctx context.Context, cube string, elements ...string) (olap.Cell, error) {
	if errors.Is(ctx.Err(), context.Canceled) {
		return olap.Cell{}, ctx.Err()
	}
	return s.cells.getCell(cube, elements...)
}

type cubes struct {
	sync.RWMutex
	cubes map[string]olap.Cube
}

func newCubes() *cubes {
	return &cubes{
		cubes: map[string]olap.Cube{},
	}
}

func (s *cubes) addCube(cube olap.Cube) error {
	s.Lock()
	defer s.Unlock()
	s.cubes[cube.Name] = cube
	return nil
}

func (s *cubes) getCube(name string) (olap.Cube, error) {
	s.RLock()
	defer s.RUnlock()
	return s.cubes[name], nil
}

type dimensions struct {
	sync.RWMutex
	dimensions map[string]olap.Dimension
}

func newDimensions() *dimensions {
	return &dimensions{
		dimensions: map[string]olap.Dimension{},
	}
}

func (s *dimensions) addDimension(dim olap.Dimension) error {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.dimensions[dim.Name]; ok {
		return olap.ErrDimensionAlreadyExists
	}
	s.dimensions[dim.Name] = dim
	return nil
}

func (s *dimensions) getDimension(name string) (olap.Dimension, error) {
	s.RLock()
	defer s.RUnlock()
	d, ok := s.dimensions[name]
	if !ok {
		return olap.Dimension{}, olap.ErrDimensionNotFound
	}
	return d, nil
}

type elements struct {
	sync.RWMutex
	elements   map[string]olap.Element
	components map[string][]string
}

func newElements() *elements {
	return &elements{
		elements:   map[string]olap.Element{},
		components: map[string][]string{},
	}
}

func (s *elements) addElement(el olap.Element) error {
	h := hash(el.Dimension, el.Name)
	s.Lock()
	defer s.Unlock()
	if _, ok := s.elements[h]; ok {
		return olap.ErrElementAlreadyExists
	}
	s.elements[h] = el
	return nil
}

func (s *elements) getElement(dim, el string) (olap.Element, error) {
	h := hash(dim, el)
	s.RLock()
	defer s.RUnlock()
	e, ok := s.elements[h]
	if !ok {
		return olap.Element{}, olap.ErrElementNotFound
	}
	return e, nil
}

func (s *elements) addComponent(tot, el olap.Element) error {
	ht := hash(tot.Dimension, tot.Name)
	he := hash(el.Dimension, el.Name)
	s.Lock()
	defer s.Unlock()
	if _, ok := s.components[ht]; !ok {
		s.components[ht] = []string{}
	}
	for _, hx := range s.components[ht] {
		if he == hx {
			return olap.ErrComponentAlreadyExists
		}
	}
	s.components[ht] = append(s.components[ht], he)
	return nil
}

func (s *elements) getComponent(dim, name string) (olap.Element, error) {
	s.RLock()
	defer s.RUnlock()
	he := hash(dim, name)
	if _, ok := s.components[he]; ok {
		return s.elements[he], nil
	}
	// TODO: code this
	panic("not implemented")
}

func (s *elements) children(dim, name string) ([]olap.Element, error) {
	h := hash(dim, name)
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.components[h]; !ok {
		return []olap.Element{}, olap.ErrComponentNotFound
	}
	els := []olap.Element{}
	for _, k := range s.components[h] {
		if e, ok := s.elements[k]; ok {
			els = append(els, e)
		}
	}
	return els, nil
}

type cells struct {
	sync.RWMutex
	cells map[string]olap.Cell
}

func newCells() *cells {
	return &cells{
		cells: map[string]olap.Cell{},
	}
}

func (s *cells) addCell(cell olap.Cell) error {
	h := hash(cell.Elements...)
	h = hash(cell.Cube, h)
	s.Lock()
	defer s.Unlock()
	s.cells[h] = cell
	return nil
}

func (s *cells) getCell(cube string, elements ...string) (olap.Cell, error) {
	h := hash(elements...)
	h = hash(cube, h)
	s.RLock()
	defer s.RUnlock()
	if c, ok := s.cells[h]; ok {
		return c, nil
	}
	return olap.Cell{}, olap.ErrCellNotFound
}

func hash(words ...string) string {
	return strings.Join(words, "#")
}

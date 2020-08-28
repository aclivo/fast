package fast

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/aclivo/olap"
)

type storage struct {
	cubes      *cubes
	dimensions *dimensions
	elements   *elements
	cells      *cells
	delay      time.Duration
}

// NewStorage creates a new fast storage.
func NewStorage(delay int64) olap.Storage {
	return &storage{
		cubes:      newCubes(),
		dimensions: newDimensions(),
		elements:   newElements(),
		cells:      newCells(),
		delay:      time.Duration(delay),
	}
}

func (s *storage) AddCube(ctx context.Context, cube olap.Cube) error {
	select {
	case <-time.After(s.delay):
		return s.cubes.addCube(cube)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *storage) GetCubeByName(ctx context.Context, name string) (olap.Cube, error) {
	select {
	case <-time.After(s.delay):
		return s.cubes.getCubeByName(name)
	case <-ctx.Done():
		return olap.Cube{}, ctx.Err()
	}
}

func (s *storage) CubeExists(ctx context.Context, name string) (bool, error) {
	select {
	case <-time.After(s.delay):
		return s.cubes.cubeExists(name)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (s *storage) AddDimension(ctx context.Context, dim olap.Dimension) error {
	select {
	case <-time.After(s.delay):
		return s.dimensions.addDimension(dim)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *storage) GetDimensionByName(ctx context.Context, name string) (olap.Dimension, error) {
	select {
	case <-time.After(s.delay):
		return s.dimensions.getDimensionByName(name)
	case <-ctx.Done():
		return olap.Dimension{}, ctx.Err()
	}
}

func (s *storage) DimensionExists(ctx context.Context, name string) (bool, error) {
	select {
	case <-time.After(s.delay):
		return s.dimensions.dimensionExists(name)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (s *storage) AddElement(ctx context.Context, el olap.Element) error {
	select {
	case <-time.After(s.delay):
		return s.elements.addElement(el)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *storage) GetElementByName(ctx context.Context, dim, el string) (olap.Element, error) {
	select {
	case <-time.After(s.delay):
		return s.elements.getElementByName(dim, el)
	case <-ctx.Done():
		return olap.Element{}, ctx.Err()
	}
}

func (s *storage) ElementExists(ctx context.Context, dim, name string) (bool, error) {
	select {
	case <-time.After(s.delay):
		return s.elements.elementExists(dim, name)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (s *storage) AddComponent(ctx context.Context, tot, el olap.Element) error {
	select {
	case <-time.After(s.delay):
		return s.elements.addComponent(tot, el)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *storage) ComponentExists(ctx context.Context, dim, name string) (bool, error) {
	select {
	case <-time.After(s.delay):
		return s.elements.componentExists(dim, name)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (s *storage) Children(ctx context.Context, dim, name string) ([]olap.Element, error) {
	select {
	case <-time.After(s.delay):
		return s.elements.children(dim, name)
	case <-ctx.Done():
		return []olap.Element{}, ctx.Err()
	}
}

func (s *storage) AddCell(ctx context.Context, cell olap.Cell) error {
	select {
	case <-time.After(s.delay):
		return s.cells.addCell(cell)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *storage) GetCellByName(ctx context.Context, cube string, elements ...string) (olap.Cell, error) {
	select {
	case <-time.After(s.delay):
		return s.cells.getCellByName(cube, elements...)
	case <-ctx.Done():
		return olap.Cell{}, ctx.Err()
	}
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

func (s *cells) getCellByName(cube string, elements ...string) (olap.Cell, error) {
	h := hash(elements...)
	h = hash(cube, h)
	s.RLock()
	defer s.RUnlock()
	if c, ok := s.cells[h]; ok {
		return c, nil
	}
	return olap.Cell{}, olap.ErrCellNotFound
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

func (s *cubes) getCubeByName(name string) (olap.Cube, error) {
	s.RLock()
	defer s.RUnlock()
	return s.cubes[name], nil
}

func (s *cubes) cubeExists(name string) (bool, error) {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.cubes[name]
	return ok, nil
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

func (s *dimensions) getDimensionByName(name string) (olap.Dimension, error) {
	s.RLock()
	defer s.RUnlock()
	d, ok := s.dimensions[name]
	if !ok {
		return olap.Dimension{}, olap.ErrDimensionNotFound
	}
	return d, nil
}

func (s *dimensions) dimensionExists(name string) (bool, error) {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.dimensions[name]
	return ok, nil
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

func (s *elements) getElementByName(dim, el string) (olap.Element, error) {
	h := hash(dim, el)
	s.RLock()
	defer s.RUnlock()
	e, ok := s.elements[h]
	if !ok {
		return olap.Element{}, olap.ErrElementNotFound
	}
	return e, nil
}

func (s *elements) elementExists(dim, name string) (bool, error) {
	h := hash(dim, name)
	s.RLock()
	defer s.RUnlock()
	_, ok := s.elements[h]
	return ok, nil
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

func (s *elements) componentExists(dim, name string) (bool, error) {
	h := hash(dim, name)
	s.RLock()
	defer s.RUnlock()
	_, ok := s.components[h]
	return ok, nil
}

func (s *elements) children(dim, name string) ([]olap.Element, error) {
	h := hash(dim, name)
	s.RLock()
	defer s.RUnlock()
	if ok, _ := s.componentExists(dim, name); !ok {
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

func hash(strs ...string) string {
	return strings.Join(strs, "#")
}

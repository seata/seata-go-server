package prophet

import (
	"errors"
	"math"
	"sort"
)

// Min finds the lowest number in a set of data
func Min(input Float64Data) (min float64, err error) {

	// Get the count of numbers in the slice
	l := input.Len()

	// Return an error if there are no numbers
	if l == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Get the first value as the starting point
	min = input.Get(0)

	// Iterate until done checking for a lower value
	for i := 1; i < l; i++ {
		if input.Get(i) < min {
			min = input.Get(i)
		}
	}
	return min, nil
}

// Max finds the highest number in a slice
func Max(input Float64Data) (max float64, err error) {

	// Return an error if there are no numbers
	if input.Len() == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Get the first value as the starting point
	max = input.Get(0)

	// Loop and replace higher values
	for i := 1; i < input.Len(); i++ {
		if input.Get(i) > max {
			max = input.Get(i)
		}
	}

	return max, nil
}

// Sum adds all the numbers of a slice together
func Sum(input Float64Data) (sum float64, err error) {

	if input.Len() == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Add em up
	for _, n := range input {
		sum += n
	}

	return sum, nil
}

// Mean gets the average of a slice of numbers
func Mean(input Float64Data) (float64, error) {

	if input.Len() == 0 {
		return 0, errors.New("Input must not be empty")
	}

	sum, _ := input.Sum()

	return sum / float64(input.Len()), nil
}

// GeometricMean gets the geometric mean for a slice of numbers
func GeometricMean(input Float64Data) (float64, error) {

	l := input.Len()
	if l == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Get the product of all the numbers
	var p float64
	for _, n := range input {
		if p == 0 {
			p = n
		} else {
			p *= n
		}
	}

	// Calculate the geometric mean
	return math.Pow(p, 1/float64(l)), nil
}

// HarmonicMean gets the harmonic mean for a slice of numbers
func HarmonicMean(input Float64Data) (float64, error) {

	l := input.Len()
	if l == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Get the sum of all the numbers reciprocals and return an
	// error for values that cannot be included in harmonic mean
	var p float64
	for _, n := range input {
		if n < 0 {
			return 0, errors.New("Input must not contain a negative number")
		} else if n == 0 {
			return 0, errors.New("Input must not contain a zero value")
		}
		p += (1 / n)
	}

	return float64(l) / p, nil
}

// copyslice copies a slice of float64s
func copyslice(input Float64Data) Float64Data {
	s := make(Float64Data, input.Len())
	copy(s, input)
	return s
}

// sortedCopy returns a sorted copy of float64s
func sortedCopy(input Float64Data) (copy Float64Data) {
	copy = copyslice(input)
	sort.Float64s(copy)
	return
}

// Median gets the median number in a slice of numbers
func Median(input Float64Data) (median float64, err error) {

	// Start by sorting a copy of the slice
	c := sortedCopy(input)

	// No math is needed if there are no numbers
	// For even numbers we add the two middle numbers
	// and divide by two using the mean function above
	// For odd numbers we just use the middle number
	l := len(c)
	if l == 0 {
		return 0, errors.New("Input must not be empty")
	} else if l%2 == 0 {
		median, _ = Mean(c[l/2-1 : l/2+1])
	} else {
		median = float64(c[l/2])
	}

	return median, nil
}

// Mode gets the mode of a slice of numbers
func Mode(input Float64Data) (mode []float64, err error) {

	// Return the input if there's only one number
	l := input.Len()
	if l == 1 {
		return input, nil
	} else if l == 0 {
		return nil, errors.New("Input must not be empty")
	}

	// Create a map with the counts for each number
	m := make(map[float64]int)
	for _, v := range input {
		m[v]++
	}

	// Find the highest counts to return as a slice
	// of ints to accomodate duplicate counts
	var current int
	for k, v := range m {

		// Depending if the count is lower, higher
		// or equal to the current numbers count
		// we return nothing, start a new mode or
		// append to the current mode
		switch {
		case v < current:
		case v > current:
			current = v
			mode = append(mode[:0], k)
		default:
			mode = append(mode, k)
		}
	}

	// Finally we check to see if there actually was
	// a mode by checking the length of the input and
	// mode against eachother
	lm := len(mode)
	if l == lm {
		return Float64Data{}, nil
	}

	return mode, nil
}

// Float64Data is a named type for []float64 with helper methods
type Float64Data []float64

// Get item in slice
func (f Float64Data) Get(i int) float64 { return f[i] }

// Len returns length of slice
func (f Float64Data) Len() int { return len(f) }

// Less returns if one number is less than another
func (f Float64Data) Less(i, j int) bool { return f[i] < f[j] }

// Swap switches out two numbers in slice
func (f Float64Data) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

// Min returns the minimum number in the data
func (f Float64Data) Min() (float64, error) { return Min(f) }

// Max returns the maximum number in the data
func (f Float64Data) Max() (float64, error) { return Max(f) }

// Sum returns the total of all the numbers in the data
func (f Float64Data) Sum() (float64, error) { return Sum(f) }

// Mean returns the mean of the data
func (f Float64Data) Mean() (float64, error) { return Mean(f) }

// Median returns the median of the data
func (f Float64Data) Median() (float64, error) { return Median(f) }

// Mode returns the mode of the data
func (f Float64Data) Mode() ([]float64, error) { return Mode(f) }

// StandardDeviation the amount of variation in the dataset
func StandardDeviation(input Float64Data) (sdev float64, err error) {
	return StandardDeviationPopulation(input)
}

// StandardDeviationPopulation finds the amount of variation from the population
func StandardDeviationPopulation(input Float64Data) (sdev float64, err error) {

	if input.Len() == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Get the population variance
	vp, _ := PopulationVariance(input)

	// Return the population standard deviation
	return math.Pow(vp, 0.5), nil
}

// PopulationVariance finds the amount of variance within a population
func PopulationVariance(input Float64Data) (pvar float64, err error) {

	v, err := _variance(input, 0)
	if err != nil {
		return 0, err
	}

	return v, nil
}

// _variance finds the variance for both population and sample data
func _variance(input Float64Data, sample int) (variance float64, err error) {

	if input.Len() == 0 {
		return 0, errors.New("Input must not be empty")
	}

	// Sum the square of the mean subtracted from each number
	m, _ := Mean(input)

	for _, n := range input {
		variance += (float64(n) - m) * (float64(n) - m)
	}

	// When getting the mean of the squared differences
	// "sample" will allow us to know if it's a sample
	// or population and wether to subtract by one or not
	return variance / float64((input.Len() - (1 * sample))), nil
}

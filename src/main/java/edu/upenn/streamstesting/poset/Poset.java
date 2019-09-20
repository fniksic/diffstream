package edu.upenn.streamstesting.poset;

import edu.upenn.streamstesting.Dependence;

import java.io.Serializable;
import java.util.*;

public class Poset<T> implements Serializable {

    private static final long serialVersionUID = 7551413373199579867L;

    private Dependence<T> dependence;

    private List<Element<T>> minimalElements = new LinkedList<>();

    private Deque<Element<T>> elements = new LinkedList<>();

    public Poset() {}

    public Poset(Dependence<T> dependence) {
        this.dependence = dependence;
    }

    public Dependence<T> getDependence() {
        return dependence;
    }

    public void setDependence(Dependence<T> dependence) {
        this.dependence = dependence;
    }

    public List<Element<T>> getMinimalElements() {
        return minimalElements;
    }

    public void setMinimalElements(List<Element<T>> minimalElements) {
        this.minimalElements = minimalElements;
    }

    public Deque<Element<T>> getElements() {
        return elements;
    }

    public void setElements(Deque<Element<T>> elements) {
        this.elements = elements;
    }

    public Element<T> allocateElement(T value) {
        Element<T> element = new Element<>(value);

        // Populate immediate predecessors
        Iterator<Element<T>> iterator = elements.descendingIterator();
        while (iterator.hasNext()) {
            Element<T> current = iterator.next();

            // Skip the element if its value is independent of the new value
            if (!dependence.test(current.getValue(), value)) {
                continue;
            }

            // Check if the element immediately precedes the new value
            boolean immediatelyPrecedes = true;
            for (Element<T> predecessor : element.getImmediatePredecessors()) {
                if (current.lessThan(predecessor)) {
                    immediatelyPrecedes = false;
                    break;
                }
            }
            if (immediatelyPrecedes) {
                element.addImmediatePredecessor(current);
            }
        }

        return element;
    }

    public void addAllocatedElement(Element<T> element) {
        for (Element<T> predecessor : element.getImmediatePredecessors()) {
            predecessor.addImmediateSuccessor(element);
        }
        elements.add(element);
        if (element.isMinimal()) {
            minimalElements.add(element);
        }
    }

    public boolean matchAndRemoveMinimal(T value) {
        Element<T> matchedElement = null;
        Iterator<Element<T>> iterator = minimalElements.iterator();
        while (iterator.hasNext()) {
            Element<T> minimalElement = iterator.next();
            if (minimalElement.getValue().equals(value)) {
                matchedElement = minimalElement;
                break;
            }
        }
        if (null != matchedElement) {
            iterator.remove();
            elements.remove(matchedElement);
            for (Element<T> successor : matchedElement.getImmediateSuccessors()) {
                successor.removeImmediatePredecessor(matchedElement);
                if (successor.isMinimal()) {
                    minimalElements.add(successor);
                }
            }
            return true;
        }
        return false;
    }

    public boolean isDependent(T value) {
        for (Element<T> element : elements) {
            if (dependence.test(element.getValue(), value)) {
                return true;
            }
        }
        return false;
    }
}
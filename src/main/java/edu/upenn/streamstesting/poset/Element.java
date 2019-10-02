package edu.upenn.streamstesting.poset;

import java.util.*;

public class Element<T> {

    private T value;

    private List<Element<T>> immediateSuccessors = new ArrayList<>();

    private List<Element<T>> immediatePredecessors = new LinkedList<>();

    public Element(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public List<Element<T>> getImmediateSuccessors() {
        return immediateSuccessors;
    }

    public List<Element<T>> getImmediatePredecessors() {
        return immediatePredecessors;
    }

    public void addImmediateSuccessor(Element<T> element) {
        immediateSuccessors.add(element);
    }

    public void removeImmediateSuccessor(Element<T> element) {
        immediateSuccessors.remove(element);
    }

    public void addImmediatePredecessor(Element<T> element) {
        immediatePredecessors.add(element);
    }

    public void removeImmediatePredecessor(Element<T> element) {
        immediatePredecessors.remove(element);
    }

    public boolean isMinimal() {
        return immediatePredecessors.isEmpty();
    }

    public boolean lessThan(Element<T> other) {
        if (this.equals(other)) {
            return false;
        }

        Set<Element<T>> visited = new HashSet<>();
        Queue<Element<T>> q = new ArrayDeque<>(Collections.singleton(this));
        while (!q.isEmpty()) {
            Element<T> current = q.remove();
            if (current.equals(other)) {
                return true;
            }
            if (!visited.contains(current)) {
                visited.add(current);
                q.addAll(current.getImmediateSuccessors());
            }
        }

        return false;
    }
}

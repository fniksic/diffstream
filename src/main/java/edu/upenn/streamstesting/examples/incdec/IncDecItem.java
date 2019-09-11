package edu.upenn.streamstesting.examples.incdec;

import edu.upenn.streamstesting.utils.Case;

public abstract class IncDecItem {

    private IncDecItem() {}

    public abstract <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase);

    /**
     * KK: Minor: It would be nice if we could make data items
     *     extend/implement from a data item class that contains a
     *     value. The rest of the items (like Hash) can be punctuation
     *     that doesn't have any value.
     **/
    public static final class Inc extends IncDecItem {

        private int value;

        public Inc(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase) {
            return incCase.apply(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Inc inc = (Inc) o;
            return value == inc.getValue();
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    public static final class Dec extends IncDecItem {

        private int value;

        public Dec(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase) {
            return decCase.apply(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Dec dec = (Dec) o;
            return value == dec.getValue();
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    public static final class Hash extends IncDecItem {

        public Hash() {}

        @Override
        public <T> T match(Case<Inc, T> incCase, Case<Dec, T> decCase, Case<Hash, T> hashCase) {
            return hashCase.apply(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}

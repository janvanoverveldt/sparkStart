package kdg.bigdata.domain;
import java.io.Serializable;

public  class Person implements Serializable {
        private String name;
        private float salary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public float getSalary() {
            return salary;
        }

        public void setSalary(float age) {
            this.salary = salary;
        }
    }


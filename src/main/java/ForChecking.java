public class ForChecking {

    public static void main(String[] args) {

        double N1 = 4;
        double N2 = 2;
        double N3 = 2;
        double C0 = 13;
        double C1 = 4;
        double C2 = 2;

        double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
        double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);

        double p = k3 * N3 / C2 + (1 - k3) * k2 * N2 / C1 + (1 - k3) * (1 - k2) * N1 / C0;

        System.out.println(p);
    }
}

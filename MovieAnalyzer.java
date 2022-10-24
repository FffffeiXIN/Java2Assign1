import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {

    public static class Movie {
        private String seriesTitle;
        private int releasedYear;
        private String certificate;
        private String runtime;
        private String genre;
        private float imdbRating;
        private String overview;
        private String metaScore;
        private String director;
        private String star1;
        private String star2;
        private String star3;
        private String star4;
        private long noofvotes;
        private String gross;

        public Movie(String seriesTitle, int releasedYear, String certificate, String runtime, String genre, float imdbRating, String overview, String metaScore, String director, String star1, String star2, String star3, String star4, long noofvotes, String gross) {
            this.seriesTitle = seriesTitle;
            this.releasedYear = releasedYear;
            this.certificate = certificate;
            this.runtime = runtime;
            this.genre = genre;
            this.imdbRating = imdbRating;
            this.overview = overview;
            this.metaScore = metaScore;
            this.director = director;
            this.star1 = star1;
            this.star2 = star2;
            this.star3 = star3;
            this.star4 = star4;
            this.noofvotes = noofvotes;
            this.gross = gross;
        }

        public int getReleasedYear() {
            return releasedYear;
        }

        public String getGenre() {
            return genre;
        }

        public void setGenre(String genre) {
            this.genre = genre;
        }

        public String[] getOrderedStars() {
            String[] stars = new String[]{star1, star2, star3, star4};
            Arrays.sort(stars);
            return stars;
        }

        public int getRuntime() {
            return Integer.parseInt(runtime.substring(0, runtime.indexOf(" ")));
        }

        public int getLengthOfOverview() {
            return overview.replaceAll("^\"|\"$", "").length();
        }

        public String getSeriesTitle() {
            return seriesTitle.replaceAll("^\"|\"$", "");
        }

        public float getimdb_Rating() {
            return imdbRating;
        }

        public String getGross() {
            return gross;
        }

        @Override
        public String toString() {
            return "Movie{"
                    + "seriesTitle='" + seriesTitle + '\''
                    + ", releasedYear=" + releasedYear
                    + ", certificate='" + certificate + '\''
                    + ",runtime='" + runtime + '\''
                    + ", genre='" + genre + '\''
                    + ", IMDB_Rating=" + imdbRating
                    + ", Overview='" + overview + '\''
                    + ", Meta_score=" + metaScore
                    + ", Director='" + director + '\''
                    + ", Star1='" + star1 + '\''
                    + ", Star2='" + star2 + '\''
                    + ", Star3='" + star3 + '\''
                    + ", Star4='" + star4 + '\''
                    + ", Noofvotes=" + noofvotes
                    + ", Gross=" + gross
                    + '}';
        }
    }

    Supplier<Stream<Movie>> movieSupplier;

    public MovieAnalyzer(String dataset_path) {
        movieSupplier = () -> {
            try {
                return Files.lines(Paths.get(dataset_path))
                        .filter(s -> s.startsWith("\""))
                        .map(s -> s.substring(s.indexOf(".jpg") + 6))
                        .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                        .map(a -> new Movie(a[0], Integer.parseInt(a[1]), a[2], a[3], a[4], Float.parseFloat(a[5]), a[6], a[7], a[8], a[9], a[10], a[11], a[12], Long.parseLong(a[13]), a[14]));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Map<Integer, Integer> getMovieCountByYear() {
        Map<Integer, Integer> res = new LinkedHashMap<>();
        movieSupplier.get().collect(Collectors.groupingBy(Movie::getReleasedYear, Collectors.counting()))
                .entrySet().stream().sorted(Map.Entry.<Integer, Long>comparingByKey().reversed())
                .forEachOrdered(e -> res.put(e.getKey(), e.getValue().intValue()));
        return res;
    }

    public Map<String, Integer> getMovieCountByGenre() {
        Map<String, Integer> temp = new LinkedHashMap<>();
        Map<String, Integer> res = new LinkedHashMap<>();
        movieSupplier.get().forEach(m -> {
            m.setGenre(m.getGenre().replace("\"", ""));
            String[] genres = m.getGenre().split(", ");
            for (int i = 0; i < genres.length; i++) {
                temp.merge(genres[i], 1, (o, n) -> o + 1);
            }
        });
        temp.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                .forEachOrdered(e -> res.put(e.getKey(), e.getValue()));
        return res;
    }

    public Map<List<String>, Integer> getCoStarCount() {
        Map<List<String>, Integer> res = new LinkedHashMap<>();

        movieSupplier.get().forEach(m -> {
            String[] stars = m.getOrderedStars();
            res.merge(Arrays.asList(stars[0], stars[1]), 1, (o, n) -> o + 1);
            res.merge(Arrays.asList(stars[0], stars[2]), 1, (o, n) -> o + 1);
            res.merge(Arrays.asList(stars[0], stars[3]), 1, (o, n) -> o + 1);
            res.merge(Arrays.asList(stars[1], stars[2]), 1, (o, n) -> o + 1);
            res.merge(Arrays.asList(stars[1], stars[3]), 1, (o, n) -> o + 1);
            res.merge(Arrays.asList(stars[2], stars[3]), 1, (o, n) -> o + 1);
        });
        return res;
    }

    public List<String> getTopMovies(int top_k, String by) {
        List<String> res = new LinkedList<>();
        if (by.equals("runtime")) {
            movieSupplier.get().sorted(Comparator.comparing(Movie::getRuntime).reversed().thenComparing(Movie::getSeriesTitle))
                    .limit(top_k).forEachOrdered(e -> res.add(e.getSeriesTitle()));
        } else if (by.equals("overview")) {
            movieSupplier.get().sorted(Comparator.comparingInt(Movie::getLengthOfOverview).reversed().thenComparing(Movie::getSeriesTitle))
                    .limit(top_k).forEachOrdered(e -> res.add(e.getSeriesTitle()));
        }
        return res;
    }

    public List<String> getTopStars(int top_k, String by) {
        class Pair1 {
            String name;
            float rating;

            public Pair1(String name, float rating) {
                this.name = name;
                this.rating = rating;
            }

            public String getName() {
                return name;
            }

            public float getRating() {
                return rating;
            }
        }

        class Pair2 {
            public String name;
            public long gross;

            public Pair2(String name, long gross) {
                this.name = name;
                this.gross = gross;
            }

            public String getName() {
                return name;
            }

            public long getGross() {
                return gross;
            }
        }
        List<String> res = new LinkedList<>();
        List<Pair1> temp1 = new ArrayList<>();
        List<Pair2> temp2 = new ArrayList<>();

        if (by.equals("rating")) {
            movieSupplier.get().forEach(e -> {
                float rating = e.getimdb_Rating();
                String[] stars = e.getOrderedStars();
                temp1.add(new Pair1(stars[0], rating));
                temp1.add(new Pair1(stars[1], rating));
                temp1.add(new Pair1(stars[2], rating));
                temp1.add(new Pair1(stars[3], rating));
            });
            temp1.stream().collect(Collectors.groupingBy(Pair1::getName, Collectors.averagingDouble(Pair1::getRating)))
                    .entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                    .limit(top_k).forEachOrdered(e -> res.add(e.getKey()));
        } else if (by.equals("gross")) {
            movieSupplier.get().filter(e -> !e.getGross().equals("")).forEach(e -> {
                long gross = Long.parseLong(e.getGross().replace("\"", "").replace(",", ""));
                String[] stars = e.getOrderedStars();
                temp2.add(new Pair2(stars[0], gross));
                temp2.add(new Pair2(stars[1], gross));
                temp2.add(new Pair2(stars[2], gross));
                temp2.add(new Pair2(stars[3], gross));
            });
            temp2.stream().collect(Collectors.groupingBy(Pair2::getName, Collectors.averagingDouble(Pair2::getGross)))
                    .entrySet().stream().sorted(Map.Entry.<String, Double>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                    .limit(top_k).forEachOrdered(e -> res.add(e.getKey()));
        }
        return res;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime) {
        List<String> res = movieSupplier.get().filter(e -> e.getGenre().contains(genre))
                .filter(e -> e.getimdb_Rating() >= min_rating)
                .filter(e -> e.getRuntime() <= max_runtime)
                .map(Movie::getSeriesTitle).sorted().toList();
        return res;
    }
}
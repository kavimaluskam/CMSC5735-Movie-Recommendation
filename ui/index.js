import Promise from "promise-polyfill";
import "whatwg-fetch";
import Vue from "vue";
import Vuetify from 'vuetify'
import Set from "es6-set";
import 'vuetify/dist/vuetify.min.css'

if (!window.Promise) {
    window.Promise = Promise;
}

Vue.use(Vuetify)



new Vue({
    el: '#app',
    data () {
        return {
            colRecommendations: [],
            loading: false,
            movies: [],
            movieNames: [],
            recommendations: [],
            recommendationLoading: false,
            search: null,
            selectedMovie: null,
            selectedTab: 0,
            selectedUser: null,
            userLoading: false,
            users: [
                // '919106',
                // '1182575',
                '387418',
                '305344',
                '2439493',
                '1664010',
                '2118461',
                '1314869',
                '1932594',
                '1639792',
                '2056022',
                '1461435',
            ],
        }
    },
    watch: {
        search (queryString) {
            queryString && this.searchMovies(queryString)
        }
    },
    methods: {
        onMovieSelect (event) {
            event.preventDefault();
            const movieName = event.target.value;
            const movieObject = this.movies.filter(item => item.movie_name === movieName)[0];
            if(movieObject) {
                const movieId = movieObject && movieObject.movie_id;
                this.recommendationLoading = true;
                fetch(`/search_similar/${movieId}`)
                    .then((response) => response.json())
                    .then((json) => {
                        this.recommendations = json;
                        this.recommendationLoading = false;
                    })
                    .catch((err) => {
                        this.error = err;
                        // TODO error handling, e.g. show msg in UI?
                    });
            }
        },
        onTabChange () {
            console.log('onTabChange');
        },
        onUserSelect (userId) {
            this.userLoading = true;
            fetch(`/get_products/${userId}`)
                .then((response) => response.json())
                .then((json) => {
                    this.colRecommendations = json;
                    this.userLoading = false;
                })
                .catch((err) => {
                    this.error = err;
                    this.userLoading = false;
                    // TODO error handling, e.g. show msg in UI?
                });
        },
        searchMovies (queryString) {
            this.loading = true;
            fetch(`/search/${queryString}`)
                .then((response) => response.json())
                .then((json) => {
                    this.movies = json;
                    this.movieNames = json.map(item => item.movie_name);
                    this.loading = false;
                })
                .catch((err) => {
                    this.error = err;
                    this.loading = false;
                    // TODO error handling, e.g. show msg in UI?
                });
            }
        },
    }
)
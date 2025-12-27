package com.github.fabriciolfj.book_server;

import com.github.fabriciolfk.book_server.grpc.*;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class BookServiceImpl extends BookServiceGrpc.BookServiceImplBase {

    private final Map<Integer, Book> bookRepository = new ConcurrentHashMap<>();
    private final AtomicInteger idGenerator = new AtomicInteger(1);

    public BookServiceImpl() {
        createSampleBooks();
    }

    private void createSampleBooks() {
        bookRepository.put(1, Book.newBuilder()
                .setId(1)
                .setTitle("Clean Code")
                .setAuthor("Robert C. Martin")
                .setIsbn("978-0132350884")
                .setCategory(BookCategory.TECHNOLOGY)
                .setPrice(47.99)
                .build());

        bookRepository.put(2, Book.newBuilder()
                .setId(2)
                .setTitle("Effective Java")
                .setAuthor("Joshua Bloch")
                .setIsbn("978-0134685991")
                .setCategory(BookCategory.TECHNOLOGY)
                .setPrice(54.99)
                .build());

        bookRepository.put(3, Book.newBuilder()
                .setId(3)
                .setTitle("1984")
                .setAuthor("George Orwell")
                .setIsbn("978-0451524935")
                .setCategory(BookCategory.FICTION)
                .setPrice(15.99)
                .build());

        idGenerator.set(4);
    }

    @Override
    public void getBook(final BookRequest request, final StreamObserver<BookResponse> responseObserver) {
        final Book book = bookRepository.get(request.getId());

        final BookResponse.Builder responseBuilder = BookResponse.newBuilder();
        if (book != null) {
            responseBuilder.setBook(book);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listBooks(final Empty request, final StreamObserver<BookListResponse> responseObserver) {
        BookListResponse response = BookListResponse.newBuilder()
                .addAllBooks(bookRepository.values())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void findBooksByCategory(final CategoryRequest request,
                                    final StreamObserver<BookListResponse> responseObserver) {
        final List<Book> filteredBooks = bookRepository.values().stream()
                .filter(book -> book.getCategory() == request.getCategory())
                .collect(Collectors.toList());

        BookListResponse response = BookListResponse.newBuilder()
                .addAllBooks(filteredBooks)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createBook(CreateBookRequest request,
                           StreamObserver<BookResponse> responseObserver) {
        int newId = idGenerator.getAndIncrement();

        final Book newBook = Book.newBuilder()
                .setId(newId)
                .setTitle(request.getTitle())
                .setAuthor(request.getAuthor())
                .setIsbn(request.getIsbn())
                .setCategory(request.getCategory())
                .setPrice(request.getPrice())
                .build();

        bookRepository.put(newId, newBook);

        final BookResponse response = BookResponse.newBuilder()
                .setBook(newBook)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}

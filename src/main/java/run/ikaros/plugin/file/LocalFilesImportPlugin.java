package run.ikaros.plugin.file;

import org.pf4j.PluginWrapper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import reactor.core.scheduler.Schedulers;
import run.ikaros.api.core.file.FileOperate;
import run.ikaros.api.infra.properties.IkarosProperties;
import run.ikaros.api.infra.utils.FileUtils;
import run.ikaros.api.plugin.BasePlugin;
import run.ikaros.api.store.entity.FileEntity;
import run.ikaros.api.store.enums.FilePlace;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LocalFilesImportPlugin extends BasePlugin {
    private static final String NAME = "PluginLocalFilesImport";
    private final IkarosProperties ikarosProperties;
    private final FileOperate fileOperate;

    public LocalFilesImportPlugin(PluginWrapper wrapper, IkarosProperties ikarosProperties,
                                  FileOperate fileOperate) {
        super(wrapper);
        this.ikarosProperties = ikarosProperties;
        this.fileOperate = fileOperate;
    }


    @Override
    public void start() {
        log.info("plugin [{}] start success", NAME);
        linkFiles();
    }

    @Override
    public void stop() {
        log.info("plugin [{}] stop success", NAME);
    }

    @Override
    public void delete() {
        log.debug("plugin [{}] delete success", NAME);
    }


    @Async
    protected void linkFiles() {
        log.info("start import links dir files.");
        long start = System.currentTimeMillis();
        Path links = ikarosProperties.getWorkDir().resolve("links");
        File linksDir = links.toFile();
        if (linksDir.isFile()) {
            throw new LocalFilesImportException("current links must dir.");
        }
        if (!linksDir.exists()) {
            linksDir.mkdirs();
            log.info("mkdir links dir, path={}", linksDir);
        }

        handleImportDirFile(linksDir);

        log.info("end import links dir files, time: {}", System.currentTimeMillis() - start);
    }

    private FileEntity handleSingle(File file) throws IOException {
        // 创建上传文件目录
        String name = file.getName();
        String filePostfix = FileUtils.parseFilePostfix(name);
        String uploadFilePath
            = FileUtils.buildAppUploadFilePath(
            ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
            filePostfix);
        File uploadFile = new File(uploadFilePath);


        try {
            Files.createLink(uploadFile.toPath(), file.toPath());
            log.debug("copy file hard link from: {}, to: {}",
                file.getAbsolutePath(), uploadFile.getAbsolutePath());
        } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            log.debug("file already exists for path: [{}].", uploadFile.getAbsolutePath());
            return null;
        } catch (SecurityException securityException) {
            // 硬链接失败则进行复制
            log.warn("file hard links fail from: {}, to: {}",
                file.getAbsolutePath(), uploadFile.getAbsolutePath());
            Files.copy(file.toPath(), uploadFile.toPath());
            log.debug("copy file from: {}, to: {}",
                file.getAbsolutePath(), uploadFile.getAbsolutePath());
        }

        FileEntity fileEntity = new FileEntity();
        fileEntity.setPlace(FilePlace.LOCAL);
        fileEntity.setName(name);
        fileEntity.setOriginalName(name);
        fileEntity.setType(FileUtils.parseTypeByPostfix(filePostfix));
        fileEntity.setUrl(
            uploadFilePath.replace(ikarosProperties.getWorkDir().toFile().getAbsolutePath(), "")
                .replace("\\", "/"));
        fileEntity.setType(FileUtils.parseTypeByPostfix(filePostfix));
        fileEntity.setOriginalPath(file.getAbsolutePath());
        fileEntity.setSize(file.length());
        fileEntity.setUpdateTime(LocalDateTime.now());
        fileEntity.setCreateTime(LocalDateTime.now());
        return fileEntity;
    }


    private void handleImportDirFile(File file) {
        if (file.isDirectory()) {
            // dir
            for (File f : Objects.requireNonNull(file.listFiles())) {
                handleImportDirFile(f);
            }
        } else {
            // file
            if (!file.exists()) {
                return;
            }

            try {
                fileOperate.existsByOriginalPath(file.getAbsolutePath())
                    .filter(exist -> !exist)
                    .mapNotNull(exists -> {
                        try {
                            return handleSingle(file);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .flatMap(fileOperate::create)
                    .subscribeOn(Schedulers.boundedElastic()).subscribe();


                // AtomicReference<Boolean> hasExists = new AtomicReference<>(false);
                // fileOperate.existsByOriginalPath(file.getAbsolutePath())
                //     .subscribeOn(Schedulers.boundedElastic())
                //     .subscribe(hasExists::set);

                // // 查询文件是否已经导入
                // if (hasExists.get()) {
                //     log.debug("skip current file operate, "
                //         + "file has exists in database for file: [{}].", file.getAbsolutePath());
                //     return;
                // }

                // // 创建上传文件目录
                // String name = file.getName();
                // String filePostfix = FileUtils.parseFilePostfix(name);
                // String uploadFilePath
                //     = FileUtils.buildAppUploadFilePath(
                //     ikarosProperties.getWorkDir().toFile().getAbsolutePath(),
                //     filePostfix);
                // File uploadFile = new File(uploadFilePath);

                // boolean isHardLink = false;
                // try {
                //     Files.createLink(file.toPath(), uploadFile.toPath());
                //     log.debug("copy file hard link from: {}, to: {}",
                //         file.getAbsolutePath(), uploadFile.getAbsolutePath());
                //     isHardLink = true;
                // } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                //     log.debug("file already exists for path: [{}].", uploadFile.getAbsolutePath());
                //     return;
                // } catch (SecurityException securityException) {
                //     // 硬链接失败则进行复制
                //     log.warn("file hard links fail from: {}, to: {}",
                //         file.getAbsolutePath(), uploadFile.getAbsolutePath());
                //     Files.copy(file.toPath(), uploadFile.toPath());
                //     log.debug("copy file from: {}, to: {}",
                //         file.getAbsolutePath(), uploadFile.getAbsolutePath());
                // }

                // FileEntity fileEntity = new FileEntity();
                // fileEntity.setPlace(FilePlace.LOCAL);
                // fileEntity.setName(name);
                // fileEntity.setOriginalName(name);
                // fileEntity.setType(FileUtils.parseTypeByPostfix(filePostfix));
                // fileEntity.setUrl(
                //     uploadFilePath.replace(SystemVarUtils.getCurrentAppDirPath(), "")
                //         .replace("\\", "/"));
                // fileEntity.setType(FileUtils.parseTypeByPostfix(filePostfix));
                // fileEntity.setOriginalPath(uploadFilePath);
                // fileOperate.create(fileEntity)
                //     .subscribeOn(Schedulers.boundedElastic()).subscribe();
                // log.debug("save file entity form original dir for file path={}",
                //     file.getAbsolutePath());
                // log.debug("success link file form path={}", file.getAbsolutePath());
            } catch (Exception e) {
                log.error("exec handleLinkDirFile exception", e);
            }
        }
    }
}
